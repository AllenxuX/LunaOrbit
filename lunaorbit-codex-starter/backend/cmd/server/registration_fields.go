package main

import (
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
)

// discoveryRegistration copies identity and business fields (room, environment, description)
// from a stored registration. Credential and security fields are deliberately not copied.
func discoveryRegistration(row registry.Registration) discovery.Registration {
	return discovery.Registration{ID: row.ID, DisplayName: row.DisplayName, BootstrapServers: row.BootstrapServers, Enabled: row.Enabled, DiscoveredClusterID: row.DiscoveredClusterID,
		IdentityVerified: row.Status == "connected" && row.LastProbeErrorCode == "" && row.LastProbeAt != nil,
		Room:             row.Room, Environment: row.Environment, Description: row.Description}
}
