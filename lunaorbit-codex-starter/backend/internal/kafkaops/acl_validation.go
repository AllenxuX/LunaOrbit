package kafkaops

import "strings"

// ACLCapabilities describes valid native Kafka resource/operation combinations.
func ACLCapabilities() map[string][]string {
	return map[string][]string{
		"TOPIC":            {"ALL", "READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE", "DESCRIBE_CONFIGS", "ALTER_CONFIGS"},
		"GROUP":            {"ALL", "READ", "DELETE", "DESCRIBE"},
		"CLUSTER":          {"ALL", "CREATE", "ALTER", "DESCRIBE", "CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE"},
		"TRANSACTIONAL_ID": {"ALL", "WRITE", "DESCRIBE"},
		"DELEGATION_TOKEN": {"ALL", "DESCRIBE"},
	}
}

func ValidACLRule(r ACLRule) bool {
	if !strings.Contains(r.Principal, ":") || strings.HasSuffix(r.Principal, ":") || len(r.Principal) > 256 || r.Host == "" || len(r.Host) > 256 || r.ResourceName == "" || len(r.ResourceName) > 249 || strings.ContainsAny(r.Principal+r.Host+r.ResourceName, "\x00\r\n") {
		return false
	}
	if r.PatternType != "LITERAL" && r.PatternType != "PREFIXED" || r.Permission != "ALLOW" && r.Permission != "DENY" {
		return false
	}
	if r.ResourceType == "CLUSTER" && (r.ResourceName != "kafka-cluster" || r.PatternType != "LITERAL") {
		return false
	}
	for _, op := range ACLCapabilities()[r.ResourceType] {
		if op == r.Operation {
			return true
		}
	}
	return false
}
