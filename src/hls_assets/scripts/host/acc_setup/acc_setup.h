#ifndef __ACC_SETUP_H__
#define __ACC_SETUP_H__

#include "common.h"
#include "host_config.h"
#include "xcl2.h"

typedef struct AccDescriptor {
    cl::CommandQueue q;

    std::vector<cl::CommandQueue> big_gs_queue;
    std::vector<cl::CommandQueue> little_gs_queue;
    cl::CommandQueue apply_queue;
    cl::CommandQueue hbm_writer_queue;

    int num_big_krnl = BIG_KERNEL_NUM;
    int num_little_krnl = LITTLE_KERNEL_NUM;

    std::vector<cl::Kernel> big_gs_krnls;
    std::vector<cl::Kernel> little_gs_krnls;
    cl::Kernel apply_krnl;
    cl::Kernel hbm_writer_krnl;

    std::vector<std::vector<cl::Event>> big_kernel_events;
    std::vector<std::vector<cl::Event>> little_kernel_events;
    cl::Event apply_kernel_event;
    cl::Event hbm_writer_event;

    cl::Context context;

    int selected_device_index = -1;
    int device_lock_fd = -1;

    std::vector<int> big_kernel_hbm_edge_id = BIG_KERNEL_HBM_EDGE_ID;
    std::vector<int> big_kernel_hbm_node_id = BIG_KERNEL_HBM_NODE_ID;

    std::vector<int> little_kernel_hbm_edge_id = LITTLE_KERNEL_HBM_EDGE_ID;
    std::vector<int> little_kernel_hbm_node_id = LITTLE_KERNEL_HBM_NODE_ID;

} AccDescriptor;

AccDescriptor initAccelerator(std::string xcl_file);

#endif
