#
# Vitis 内核的 Makefile (多内核版本)
#

ifeq ($(TARGET),$(filter $(TARGET), sw_emu hw_emu))
CLFLAGS += -DEMULATION
endif

# --- 配置项 ---
VPP := v++
XCLBIN_DIR := ./xclbin
EMCONFIG_FILE := ./emconfig.json

# 1. 在这里定义您所有的内核名称。
#    这是将来您唯一需要修改的变量。
KERNEL_NAMES := graphyflow_little graphyflow_big apply_kernel big_merger little_merger little_prop_loader big_prop_loader

# 2. 定义最终输出的二进制文件的名称。
XCLBIN_NAME := graphyflow_kernels

# --- 文件名自动生成 ---

# 根据 KERNEL_NAMES 列表，自动生成所有内核对象 (.xo) 文件的列表。
# 例如: "graphyflow_big" -> "./xclbin/graphyflow_big.hw.xo"
KERNEL_XOS := $(patsubst %,$(XCLBIN_DIR)/%.$(TARGET).xo,$(KERNEL_NAMES))

# 定义最终 .xclbin 文件的完整路径。
XCLBIN_FILE := $(XCLBIN_DIR)/$(XCLBIN_NAME).$(TARGET).xclbin

# if defined WAVE then add -g for debug symbols
ifdef WAVE
CLFLAGS += -g
LDFLAGS_VPP += -g
endif

# --- 编译器和链接器参数 ---

# VPP 在编译 .xo 文件时使用的参数。
# 注意：特定的 "--kernel <名称>" 参数现在被移到了编译规则内部。
CLFLAGS += -Iscripts/kernel
CLFLAGS += -Iscripts/host
CLFLAGS += -I$(XILINX_XRT)/include
CLFLAGS += -I$(XILINX_VITIS)/include
CLFLAGS += -O3
CLFLAGS += --kernel_frequency=230

# VPP 在链接 .xclbin 文件时使用的参数。
LDFLAGS_VPP += --config ./system.cfg
LDFLAGS_VPP += -Iscripts/kernel
LDFLAGS_VPP += -Iscripts/host
LDFLAGS_VPP += -I$(XILINX_XRT)/include
LDFLAGS_VPP += -I$(XILINX_VITIS)/include
LDFLAGS_VPP += --xp prop:solution.kernel_compiler_margin=10%
LDFLAGS_VPP += -O3
LDFLAGS_VPP += --kernel_frequency=230


# --- 构建规则 ---

# 默认目标
all: $(XCLBIN_FILE)

# 3. 链接规则：将所有内核对象 (.xo) 文件链接成一个二进制容器 (.xclbin)。
#    此规则依赖于 KERNEL_XOS 变量中定义的所有 .xo 文件。
$(XCLBIN_FILE): $(KERNEL_XOS)
	@echo "==> 正在将所有内核链接到 xclbin 文件: $@"
	$(VPP) -l -t $(TARGET) --platform $(DEVICE) $(LDFLAGS_VPP) -o $@ $^

# 4. 模式规则：将任何内核源文件 (.cpp) 编译成对应的内核对象文件 (.xo)。
#    这一个规则就能同时处理 graphyflow_big.cpp 和 graphyflow_little.cpp。
#    '$*' 是一个特殊变量，代表文件名中的“主干”部分 (例如 "graphyflow_big")。
$(XCLBIN_DIR)/%.$(TARGET).xo: scripts/kernel/%.cpp
	@echo "==> 正在编译内核: $<"
	@mkdir -p $(XCLBIN_DIR)
	$(VPP) -c -t $(TARGET) --platform $(DEVICE) $(CLFLAGS) --kernel $* -o $@ $<

# 用于生成硬件仿真配置的规则。
emconfig:
	emconfigutil --platform $(DEVICE) --od .

# 用于清理所有生成文件的规则。
clean:
	@echo "==> 正在清理构建生成的文件"
	rm -rf $(XCLBIN_DIR) $(EMCONFIG_FILE)

# 声明伪目标 (这些目标不是实际的文件名，而是操作名称)。
.PHONY: all emconfig clean