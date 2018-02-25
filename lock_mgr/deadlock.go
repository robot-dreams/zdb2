package lock_mgr

import (
	"time"
)

const deadlockDetectorPeriod = 1 * time.Second

func (lm *lockManager) startDeadlockDetector() {
	for {
		<-time.After(deadlockDetectorPeriod)
		lm.findAndMarkDeadlock()
	}
}

func (lm *lockManager) findAndMarkDeadlock() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	waitGraph := lm.buildWaitGraph()
	clientID, ok := findCycleNode(waitGraph)
	if ok {
		lm.clientToPendingRequest[clientID].deadlockDetected = true
		lm.clientToPendingRequest[clientID].cond.Signal()
	}
}

func (lm *lockManager) buildWaitGraph() map[string][]string {
	result := make(map[string][]string)
	for _, lock := range lm.lockIDToLock {
		for _, holder := range lock.holders {
			for _, pending := range lock.pending {
				result[pending.clientID] = append(
					result[pending.clientID],
					holder.clientID)
			}
		}
	}
	return result
}

func findCycleNode(graph map[string][]string) (string, bool) {
	visited := make(map[string]bool)
	for node := range graph {
		if visited[node] {
			continue
		}
		cycleNode, ok := findCycleNodeRecursive(node, graph, visited)
		if ok {
			return cycleNode, ok
		}
	}
	return "", false
}

func findCycleNodeRecursive(
	node string,
	graph map[string][]string,
	visited map[string]bool,
) (string, bool) {
	if visited[node] {
		return node, true
	}
	visited[node] = true
	for _, neighbor := range graph[node] {
		cycleNode, ok := findCycleNodeRecursive(neighbor, graph, visited)
		if ok {
			return cycleNode, ok
		}
	}
	return "", false
}
