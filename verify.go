package dbverify

import (
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

func Verify(targets []pgx.ConnConfig, includeTables, excludeTables []string) error {
	log.Infof("Verifying %d targets", len(targets))
	for i, target := range targets {
		log.Infof("[%d] %s", i, target.Host)
	}
	return nil
}
