package registry

import "lunaorbit/backend/internal/kafkaops"

func filterTopics(in []kafkaops.Topic, allowed func(string) bool) []kafkaops.Topic {
	out := []kafkaops.Topic{}
	for _, v := range in {
		if allowed(v.Name) {
			out = append(out, v)
		}
	}
	return out
}
func filterGroups(in []kafkaops.Group, allowed func(string) bool) []kafkaops.Group {
	out := []kafkaops.Group{}
	for _, v := range in {
		if allowed(v.ID) {
			out = append(out, v)
		}
	}
	return out
}
