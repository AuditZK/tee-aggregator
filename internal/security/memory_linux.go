//go:build linux

package security

import (
	"os"
	"strconv"
	"strings"
	"syscall"

	"go.uber.org/zap"
)

// MemoryProtection hardens memory security in the enclave.
type MemoryProtection struct {
	logger *zap.Logger
}

// NewMemoryProtection creates a new memory protection service.
func NewMemoryProtection(logger *zap.Logger) *MemoryProtection {
	return &MemoryProtection{logger: logger}
}

// Apply applies all memory protection measures.
func (m *MemoryProtection) Apply() {
	m.DisableCoreDumps()
	m.CheckPtraceProtection()
	m.CheckMlock()
}

// DisableCoreDumps sets RLIMIT_CORE to 0 to prevent core dumps.
func (m *MemoryProtection) DisableCoreDumps() {
	var rLimit syscall.Rlimit
	rLimit.Cur = 0
	rLimit.Max = 0
	if err := syscall.Setrlimit(syscall.RLIMIT_CORE, &rLimit); err != nil {
		m.logger.Warn("failed to disable core dumps", zap.Error(err))
	} else {
		m.logger.Info("core dumps disabled")
	}
}

// CheckPtraceProtection checks the Yama ptrace scope.
func (m *MemoryProtection) CheckPtraceProtection() bool {
	data, err := os.ReadFile("/proc/sys/kernel/yama/ptrace_scope")
	if err != nil {
		m.logger.Warn("cannot read ptrace scope", zap.Error(err))
		return false
	}

	scope, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return false
	}

	if scope < 2 {
		m.logger.Warn("ptrace scope is low, recommend >= 2", zap.Int("scope", scope))
		return false
	}

	m.logger.Info("ptrace protection adequate", zap.Int("scope", scope))
	return true
}

// CheckMlock checks if the process can lock memory.
func (m *MemoryProtection) CheckMlock() bool {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		m.logger.Warn("cannot read process status", zap.Error(err))
		return false
	}

	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmLck:") {
			m.logger.Info("mlock status", zap.String("vmlock", strings.TrimSpace(line)))
			return true
		}
	}

	return false
}
