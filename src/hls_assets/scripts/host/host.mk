# Compiler
CXX := g++

# --- Configuration ---

# Executable name (can be passed from a top-level Makefile)
EXECUTABLE ?= graphyflow_host

# Optional compile-time profiling (no runtime overhead when disabled).
HOST_PROFILE ?= 0
GRAPH_PREPROCESS_PROFILE ?= 0
DST_SHUFFLE_MODE ?=
DST_SHUFFLE_BLOCK_SIZE ?=
BIG_EDGE_PER_MS ?= 280000
LITTLE_EDGE_PER_MS ?= 1040000

# Top-level directory for host source code
HOST_DIR := scripts/host

# --- Automatic File Discovery ---

# Use the shell's 'find' command to recursively find all .cpp files
# This automatically includes files in subdirectories like 'acc_setup'.
HOST_SRCS := $(shell find $(HOST_DIR) -name '*.cpp')

# Generate a list of object files (.o) from the source files list
# e.g., "scripts/host/host.cpp" becomes "scripts/host/host.o"
OBJECTS := $(HOST_SRCS:.cpp=.o)
DEPS := $(OBJECTS:.o=.d)

# --- Compiler and Linker Flags ---

# Include directories
CXXFLAGS := -I$(HOST_DIR)
CXXFLAGS += -Iscripts/kernel
CXXFLAGS += -I$(XILINX_XRT)/include
CXXFLAGS += -I$(XILINX_VITIS)/include
CXXFLAGS += -I$(XILINX_HLS)/include

ifeq ($(TARGET),$(filter $(TARGET), sw_emu hw_emu))
CXXFLAGS += -DEMULATION
endif
ifeq ($(TARGET),hw_emu)
CXXFLAGS += -DGRAPHYFLOW_HW_EMU_LIMIT_MAX_DST
endif

# Compiler flags
CXXFLAGS += -std=c++17 -O3 -Wall -g
CXXFLAGS += -MMD -MP

ifeq ($(HOST_PROFILE),1)
CXXFLAGS += -DENABLE_HOST_PROFILE
endif
ifeq ($(GRAPH_PREPROCESS_PROFILE),1)
CXXFLAGS += -DENABLE_GRAPH_PREPROCESS_PROFILE
endif

ifneq ($(DST_SHUFFLE_MODE),)
CXXFLAGS += -DDST_SHUFFLE_MODE=$(DST_SHUFFLE_MODE)
endif
ifneq ($(DST_SHUFFLE_BLOCK_SIZE),)
CXXFLAGS += -DDST_SHUFFLE_BLOCK_SIZE=$(DST_SHUFFLE_BLOCK_SIZE)
endif

# Repartition throughput defaults are compiled into the host binary so emitted
# projects can tune them via make variables.
CXXFLAGS += -DBIG_EDGE_PER_MS=$(BIG_EDGE_PER_MS)
CXXFLAGS += -DLITTLE_EDGE_PER_MS=$(LITTLE_EDGE_PER_MS)

# Linker flags
LDFLAGS := -L$(XILINX_XRT)/lib
LDFLAGS += -lOpenCL -lxrt_coreutil -lstdc++ -lrt -pthread -Wl,--export-dynamic

# --- Build Rules ---

all: $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	@echo "==> Linking executable: $@"
	$(CXX) $(OBJECTS) -o $(EXECUTABLE) $(LDFLAGS)

%.o: %.cpp
	@echo "==> Compiling: $<"
	$(CXX) $(CXXFLAGS) -c $< -o $@

-include $(DEPS)

clean:
	@echo "==> Cleaning up generated files"
	rm -f $(EXECUTABLE) $(OBJECTS) $(DEPS)

.PHONY: all clean
