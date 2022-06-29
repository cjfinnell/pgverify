package pgverify

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

var _ pgx.Logger = (*pgxLogger)(nil)

type pgxLogger struct {
	l logrus.FieldLogger
}

func (p *pgxLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	parsedLevel, err := logrus.ParseLevel(level.String())
	if err != nil {
		parsedLevel = logrus.ErrorLevel
	}

	p.l.WithFields(data).Log(parsedLevel, msg)
}
