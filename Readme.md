# pomodoro

Tiny daemon der nach Cron-Schedule START/PAUSE an ntfy schickt und über ein
Control-Topic per `ON`/`OFF`-Message ein- und ausgeschaltet werden kann.

## Aufbau

* Ein Go-Binary, statisch, ~6 MB. Läuft `FROM scratch` im Docker-Image.
* Cron-Scheduler aus `robfig/cron/v3`, Standard 5-Feld-Syntax.
* Pro Cron-Event:
  1. Poll des Control-Topics (`?poll=1&since=<last_poll_unix>`)
  2. Letzte `ON`/`OFF`-Message aus dem Resultat übernehmen → In-Memory-State
  3. Wenn `ON`: Publish mit Custom Sequence-ID (`p<unix-ts>`) ans Output-Topic
  4. Removal basierend auf `removal.mode`:
     * `clear`/`delete`: Timer pro Notification → Clear/Delete nach `delay_seconds`
     * `clear_on_next`/`delete_on_next`: Vorherige Notification sofort löschen, + Fallback-Timer
       für die letzte Notification (`delay_seconds`). Z.B. nach 1 Stunde (3600s) damit
       am Tagesende keine stale Notifications übrig bleiben.
* State default `ON` beim Start, nicht persistent.

## Quickstart

```bash
# 1. Config anlegen
cp config.example.yml config.yml
$EDITOR config.yml

# 2. Auth-Credentials ins .env (NICHT ins YAML)
cat > .env <<EOF
NTFY_USERNAME=pomodoro-bot
NTFY_PASSWORD=...
EOF

# 3. Starten
docker compose up -d --build
docker compose logs -f pomodoro
```

## Lokal bauen (ohne Docker)

```bash
go build -o pomodoro .
./pomodoro -config config.yml
```

## Bedienen

Daemon einschalten / ausschalten:

```bash
# über die ntfy CLI oder curl
curl -u "$NTFY_USERNAME:$NTFY_PASSWORD" -d "OFF" https://ntfy.danzl.it/pomodoro-control
curl -u "$NTFY_USERNAME:$NTFY_PASSWORD" -d "ON"  https://ntfy.danzl.it/pomodoro-control
```

Die Commands werden beim nächsten geplanten Cron-Event abgeholt — also keine
sofortige Reaktion, sondern frühestens beim nächsten START/PAUSE-Zeitpunkt.