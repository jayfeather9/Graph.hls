#ifndef __FPGA_EXECUTOR_H__
#define __FPGA_EXECUTOR_H__

#include "common.h"
#include <vector>

// 通用的 FPGA 执行函数。
// 它通过 AlgorithmHost 类来处理所有与具体算法相关的操作。
std::vector<int> run_fpga_kernel(const std::string &xclbin_path,
                                 const GraphCSR &graph, int start_node,
                                 double &total_kernel_time_sec,
                                 int &iter_count);

#endif // __FPGA_EXECUTOR_H__
