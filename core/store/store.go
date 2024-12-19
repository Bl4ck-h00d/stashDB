package store

import (
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/types"
)

// Factory
func NewStore(dbType, dbPath string) (types.Store, error) {
	switch dbType {
	case "bolt":
		return NewBoltStore(dbPath)

	default:
		return nil, fmt.Errorf("unsupported database type: %v", dbType)
	}
}
