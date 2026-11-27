package config

import (
	"strings"
	"testing"
	"time"
)

func TestConnectorValidateWithoutHeartbeat(t *testing.T) {
	cfg := Connector{}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("ordinary configuration should be valid: %v", err)
	}
}

func TestConnectorValidateRejectsHeartbeat(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			cfg := Connector{}
			cfg.CDC.Heartbeat.Table.Name = "heartbeat"
			cfg.CDC.Heartbeat.Interval = interval

			err := cfg.Validate()
			if err == nil {
				t.Fatal("heartbeat-enabled configuration should be rejected")
			}
			for _, want := range []string{"cdc.heartbeat.table", "disabled", "v1.11.14", "auto-acks", "ordered target sink", "unapplied changes"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q should contain %q", err, want)
				}
			}
		})
	}
}
