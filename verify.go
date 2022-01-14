package dbverify

import (
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

func Verify(targets []pgx.ConnConfig, includeTables, excludeTables []string) error {
	log.Infof("Verifying %d targets", len(targets))

	// First check that we can connect to every specified target database.
	conns := make(map[int]*pgx.Conn)
	for i, target := range targets {
		conn, err := pgx.Connect(target)
		if err != nil {
			return err
		}
		defer conn.Close()
		conns[i] = conn
	}

	// Then query each target database in parallel to generate table hashes.
	var doneChannels []chan struct{}
	for i, conn := range conns {
		done := make(chan struct{})
		go generateTableHashes(i, conn, includeTables, excludeTables, done)
		doneChannels = append(doneChannels, done)
	}
	for _, done := range doneChannels {
		<-done
	}

	return nil
}

func generateTableHashes(targetIndex int, conn *pgx.Conn, explicitTables, excludeTables []string, done chan struct{}) {
	// TODO
	close(done)
}
