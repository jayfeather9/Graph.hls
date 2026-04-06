SHELL           := /bin/bash

COMMON_REPO     = ./
ABS_COMMON_REPO = $(shell readlink -f $(COMMON_REPO))
SCRIPTS_PATH    = ./scripts

# Remove unused targets from .PHONY
.PHONY: all clean cleanall exe emconfig

include $(SCRIPTS_PATH)/help.mk
include $(SCRIPTS_PATH)/utils.mk

include global_para.mk

# Use SCRIPTS_PATH for consistency

# Remove non-existent makefiles
# include autogen/autogen.mk
# include acc_template/acc.mk

# Include our new kernel makefile
include $(SCRIPTS_PATH)/kernel/kernel.mk

include $(SCRIPTS_PATH)/host/host.mk 

# This include seems to be for Vitis 1.0 examples, not needed here
# include $(SCRIPTS_PATH)/bitstream.mk
include $(SCRIPTS_PATH)/clean.mk

# Update the 'all' rule to depend on the .xclbin file, the host executable, and emconfig
all: $(XCLBIN_FILE) $(EXECUTABLE) emconfig

exe: $(EXECUTABLE)
