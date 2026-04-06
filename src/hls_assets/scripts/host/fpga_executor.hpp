#ifndef FPGA_EXECUTOR_HPP
#define FPGA_EXECUTOR_HPP

#include "common.h"
#include "graph_loader.hpp"
#include "host_config.hpp"

#include "xcl2.hpp"

class FPGAExecutor {
public:
    FPGAExecutor() = default;
    ~FPGAExecutor() = default;

    bool initialize(const std::string& xclbin_path);
    void run(const GraphLoader& loader);

private:
    cl::Context context;
    cl::CommandQueue q;
    cl::Kernel kernel;
};

#endif // FPGA_EXECUTOR_HPP
