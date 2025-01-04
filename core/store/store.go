package store

import (
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/types"
)

// Factory
func NewStore(dbType, dataDir string) (types.Store, error) {
	switch dbType {
	case "bolt":
		return NewBoltStore(dataDir)

	default:
		return nil, fmt.Errorf("unsupported database type: %v", dbType)
	}
}
