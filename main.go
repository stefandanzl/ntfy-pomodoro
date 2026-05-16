// pomodoro — a tiny ntfy-driven pomodoro timer daemon.
//
// Schedules messages to ntfy topics on a cron, pollable ON/OFF/SKIP state via a
// control topic, and optionally clears or deletes published notifications
// after a delay so the topic stays tidy.
package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	// Embed the timezone database into the binary so we don't need /usr/share/zoneinfo
	// in the container. Lets us run FROM scratch.
	_ "time/tzdata"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// ---------- Config ----------

type Config struct {
	Server       string        `yaml:"server"`
	Auth         AuthConfig    `yaml:"auth"`
	ControlTopic string        `yaml:"control_topic"`
	Removal      RemovalConfig `yaml:"removal"`
	Jobs         []JobConfig   `yaml:"jobs"`
}

type AuthConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Token    string `yaml:"token"`
}

type RemovalConfig struct {
	Mode         string `yaml:"mode"`          // "clear" | "delete" | "none" | "clear_on_next" | "delete_on_next"
	DelaySeconds int    `yaml:"delay_seconds"` // 0 disables removal
}

type JobConfig struct {
	Cron    string `yaml:"cron"`
	Topic   string `yaml:"topic"`
	Message string `yaml:"message"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Env overrides — secrets shouldn't have to live in YAML.
	if p := os.Getenv("NTFY_PASSWORD"); p != "" {
		cfg.Auth.Password = p
	}
	if u := os.Getenv("NTFY_USERNAME"); u != "" {
		cfg.Auth.Username = u
	}
	if t := os.Getenv("NTFY_TOKEN"); t != "" {
		cfg.Auth.Token = t
	}

	if cfg.Server == "" {
		return nil, errors.New("server is required")
	}
	if cfg.ControlTopic == "" {
		return nil, errors.New("control_topic is required")
	}
	if len(cfg.Jobs) == 0 {
		return nil, errors.New("at least one job is required")
	}
	if cfg.Removal.Mode == "" {
		cfg.Removal.Mode = "clear"
	}
	switch cfg.Removal.Mode {
	case "clear", "delete", "none", "clear_on_next", "delete_on_next":
	default:
		return nil, fmt.Errorf("removal.mode must be one of clear|delete|none|clear_on_next|delete_on_next, got %q", cfg.Removal.Mode)
	}
	return &cfg, nil
}

// ---------- State ----------

// State holds the in-memory ON/OFF flag, an optional skip-until timestamp,
// the timestamp of the last poll against the control topic, and a pointer to
// the most recently published notification.  Default is ON at startup.
type State struct {
	mu           sync.Mutex
	enabled      bool
	skipUntil    time.Time // if non-zero, overrides enabled→false until this time
	lastPoll     time.Time
	lastPubTopic string
	lastPubSeq   string
	loc          *time.Location // for computing "start of next day"
}

func NewState(loc *time.Location) *State {
	return &State{enabled: true, lastPoll: time.Now(), loc: loc}
}

func (s *State) LastPoll() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPoll
}

// SwapLastPub atomically replaces the "last published" pointer with the given
// topic/seqID and returns whatever was there before (empty strings on first call).
func (s *State) SwapLastPub(topic, seqID string) (prevTopic, prevSeq string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prevTopic, prevSeq = s.lastPubTopic, s.lastPubSeq
	s.lastPubTopic, s.lastPubSeq = topic, seqID
	return
}

// IsEnabled returns true if notifications should be published right now.
// An active (non-expired) skip overrides enabled to false and auto-clears when expired.
func (s *State) IsEnabled(log *slog.Logger) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().In(s.loc)
	if !s.skipUntil.IsZero() {
		if !now.Before(s.skipUntil) {
			// Skip period has passed — clear it.
			log.Info("resetting skipping from value " + s.skipUntil.String())
			s.skipUntil = time.Time{}
		} else {
			log.Info("SKIP active until " + s.skipUntil.String())
			return false
		}
	}
	return s.enabled
}

// Apply processes a batch of control messages in chronological order, updates
// the lastPoll timestamp.  Unknown message bodies are ignored.
// Commands: ON, OFF, SKIP [days]
func (s *State) Apply(cmds []string, newLastPoll time.Time, log *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPoll = newLastPoll

	for _, c := range cmds {
		cmd := strings.ToUpper(strings.TrimSpace(c))

		switch {
		case cmd == "ON":
			s.enabled = true
			s.skipUntil = time.Time{} // cancel any active skip

		case cmd == "OFF":
			s.enabled = false
			s.skipUntil = time.Time{} // cancel any active skip

		case cmd == "SKIP" || strings.HasPrefix(cmd, "SKIP "):
			additionalDays := 0
			if parts := strings.Fields(c); len(parts) > 1 {
				if d, err := strconv.Atoi(parts[1]); err == nil && d > 0 {
					additionalDays = d
				}
			}
			now := time.Now().In(s.loc)
			nextMidnight := time.Date(
				now.Year(), now.Month(), now.Day()+1,
				0, 0, 0, 0, s.loc,
			)
			s.skipUntil = nextMidnight.AddDate(0, 0, additionalDays)
			log.Info("Set new skipUntil for " + s.skipUntil.String())
		}
	}
}

// ---------- ntfy client ----------

type Client struct {
	server string
	auth   string // pre-encoded "Basic xxx" or "Bearer xxx" header value, or empty
	http   *http.Client
}

func NewClient(server string, auth AuthConfig) *Client {
	c := &Client{
		server: strings.TrimRight(server, "/"),
		http:   &http.Client{Timeout: 30 * time.Second},
	}
	// Token wins over Basic if both are configured.
	switch {
	case auth.Token != "":
		c.auth = "Bearer " + auth.Token
	case auth.Username != "":
		c.auth = "Basic " + base64.StdEncoding.EncodeToString(
			[]byte(auth.Username+":"+auth.Password))
	}
	return c
}

func (c *Client) do(method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	return c.http.Do(req)
}

// Poll fetches messages from `topic` published since `since`.  Returns message
// bodies in chronological order plus the cutoff timestamp to use for the next
// poll (one second past the last seen message — avoids re-reading the boundary).
func (c *Client) Poll(topic string, since time.Time) ([]string, time.Time, error) {
	url := fmt.Sprintf("%s/%s/json?poll=1&since=%d", c.server, topic, since.Unix())
	resp, err := c.do("GET", url, nil)
	if err != nil {
		return nil, since, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, since, fmt.Errorf("poll: status %d", resp.StatusCode)
	}

	var msgs []string
	latest := since
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var m struct {
			Event   string `json:"event"`
			Message string `json:"message"`
			Time    int64  `json:"time"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			continue
		}
		if m.Event != "message" {
			continue
		}
		msgs = append(msgs, m.Message)
		if t := time.Unix(m.Time, 0); t.After(latest) {
			latest = t
		}
	}
	return msgs, latest.Add(time.Second), nil
}

// Publish posts a message body to topic/seqID.  Using a custom sequence ID via
// the URL path lets us address the message later for clear/delete without
// parsing the POST response.
func (c *Client) Publish(topic, seqID, message string) error {
	url := fmt.Sprintf("%s/%s/%s", c.server, topic, seqID)
	resp, err := c.do("POST", url, strings.NewReader(message))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("publish: status %d", resp.StatusCode)
	}
	return nil
}

// Remove clears or deletes a previously published message by sequence ID.
func (c *Client) Remove(topic, seqID, mode string) error {
	var method, suffix string
	switch mode {
	case "clear":
		method, suffix = "PUT", "/clear"
	case "delete":
		method, suffix = "DELETE", ""
	default:
		return fmt.Errorf("unknown removal mode %q", mode)
	}
	url := fmt.Sprintf("%s/%s/%s%s", c.server, topic, seqID, suffix)
	resp, err := c.do(method, url, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("remove: status %d", resp.StatusCode)
	}
	return nil
}

// ---------- Job runner ----------

// runJob is invoked by the cron scheduler for each scheduled entry.
func runJob(client *Client, state *State, cfg *Config, job JobConfig, log *slog.Logger) {
	log = log.With("topic", job.Topic, "message", job.Message)

	// 1. Poll for new control commands.
	since := state.LastPoll()
	cmds, newSince, err := client.Poll(cfg.ControlTopic, since)
	if err != nil {
		log.Error("poll failed; continuing with last known state", "err", err)
	}
	for _, c := range cmds {
		log.Info("control command", "cmd", c)
	}

	// Apply ON/OFF/SKIP commands (mutates state internally)
	state.Apply(cmds, newSince, log)

	// Check if we are allowed to publish
	if !state.IsEnabled(log) {
		log.Info("disabled — skipping publish")
		return
	}

	// 2. Publish with a custom sequence ID (Unix timestamp; topic-scoped so
	//    different jobs at the same second on different topics don't collide).
	seqID := fmt.Sprintf("p%d", time.Now().Unix())
	if err := client.Publish(job.Topic, seqID, job.Message); err != nil {
		log.Error("publish failed", "err", err)
		return
	}
	log.Info("published", "seq", seqID)

	// 3. Removal — either time-delayed (clear/delete) or triggered by the
	//    next publish (clear_on_next/delete_on_next).
	switch cfg.Removal.Mode {
	case "none":
		return

	case "clear", "delete":
		if cfg.Removal.DelaySeconds <= 0 {
			return
		}
		mode := cfg.Removal.Mode
		delay := time.Duration(cfg.Removal.DelaySeconds) * time.Second
		time.AfterFunc(delay, func() {
			if err := client.Remove(job.Topic, seqID, mode); err != nil {
				log.Error("remove failed", "err", err, "seq", seqID, "mode", mode)
				return
			}
			log.Info("removed", "seq", seqID, "mode", mode)
		})

	case "clear_on_next", "delete_on_next":
		action := "clear"
		if cfg.Removal.Mode == "delete_on_next" {
			action = "delete"
		}
		prevTopic, prevSeq := state.SwapLastPub(job.Topic, seqID)
		if prevSeq == "" {
			log.Info("no previous notification yet — nothing to remove")
			return
		}
		if err := client.Remove(prevTopic, prevSeq, action); err != nil {
			log.Error("remove previous failed",
				"err", err, "seq", prevSeq, "topic", prevTopic, "mode", action)
			return
		}
		log.Info("removed previous", "seq", prevSeq, "topic", prevTopic, "mode", action)
	}
}

// ---------- main ----------

func main() {
	cfgPath := flag.String("config", "config.yml", "path to config file")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Error("config load failed", "err", err, "path", *cfgPath)
		os.Exit(1)
	}
	log.Info("config loaded",
		"server", cfg.Server,
		"control_topic", cfg.ControlTopic,
		"jobs", len(cfg.Jobs),
		"tz", time.Local.String(),
		"removal_mode", cfg.Removal.Mode,
		"removal_delay_s", cfg.Removal.DelaySeconds,
	)

	client := NewClient(cfg.Server, cfg.Auth)
	state := NewState(time.Local)

	c := cron.New(cron.WithLocation(time.Local))
	for _, job := range cfg.Jobs {
		job := job // capture for closure
		id, err := c.AddFunc(job.Cron, func() {
			runJob(client, state, cfg, job, log)
		})
		if err != nil {
			log.Error("invalid cron expression", "cron", job.Cron, "err", err)
			os.Exit(1)
		}
		log.Info("scheduled", "id", id, "cron", job.Cron,
			"topic", job.Topic, "message", job.Message)
	}
	c.Start()
	log.Info("pomodoro daemon started")
	client.Publish(cfg.ControlTopic, "test", "Pomodoro Cron Server started")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info("shutdown signal received")

	ctx := c.Stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		log.Warn("scheduler shutdown timed out")
	}
	log.Info("bye")
}
