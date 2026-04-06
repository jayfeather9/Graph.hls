#include "acc_setup.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <sstream>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <vector>

namespace {

std::string device_lock_dir() {
    const char *dir_env = std::getenv("GRAPHYFLOW_DEVICE_LOCK_DIR");
    std::string dir =
        (dir_env && dir_env[0]) ? std::string(dir_env) : "/tmp/graphyflow_device_locks";

    // Best-effort: ensure the directory exists.
    if (::mkdir(dir.c_str(), 0777) != 0 && errno != EEXIST) {
        // If we can't create it, fall back to /tmp.
        dir = "/tmp";
    }
    return dir;
}

int try_acquire_device_lock(size_t device_index) {
    const std::string dir = device_lock_dir();
    std::string lock_path =
        dir + "/graphyflow_device_" + std::to_string(device_index) + ".lock";
    int fd = open(lock_path.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        return -1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

void release_device_lock(int fd) {
    if (fd >= 0) {
        flock(fd, LOCK_UN);
        close(fd);
    }
}

bool is_emulation_mode() {
    const char *mode = std::getenv("XCL_EMULATION_MODE");
    return mode != nullptr && mode[0] != '\0';
}

} // namespace

AccDescriptor initAccelerator(const std::string xclbin_path) {
    AccDescriptor acc;

    const auto devices = xcl::get_xil_devices();
    if (devices.empty()) {
        std::cerr << "Failed to find any Xilinx device." << std::endl;
        std::exit(EXIT_FAILURE);
    }

    std::optional<size_t> forced_device_index;
    if (const char *idx_env = std::getenv("GRAPHYFLOW_DEVICE_INDEX")) {
        if (idx_env[0] != '\0') {
            char *end = nullptr;
            errno = 0;
            const long parsed = std::strtol(idx_env, &end, 10);
            if (errno != 0 || end == idx_env || *end != '\0' || parsed < 0) {
                std::cerr << "Invalid GRAPHYFLOW_DEVICE_INDEX='" << idx_env
                          << "' (expected non-negative integer)" << std::endl;
                std::exit(EXIT_FAILURE);
            }
            forced_device_index = static_cast<size_t>(parsed);
            if (*forced_device_index >= devices.size()) {
                std::cerr << "GRAPHYFLOW_DEVICE_INDEX=" << *forced_device_index
                          << " out of range (devices=" << devices.size() << ")"
                          << std::endl;
                std::exit(EXIT_FAILURE);
            }
            std::cout << "[INFO] Forcing device index " << *forced_device_index
                      << " via GRAPHYFLOW_DEVICE_INDEX" << std::endl;
        }
    }

    const auto fileBuf = xcl::read_binary_file(xclbin_path);
    cl::Program::Binaries bins{{fileBuf.data(), fileBuf.size()}};

    std::vector<std::string> device_failures;

    const size_t dev_begin = forced_device_index.value_or(0);
    const size_t dev_end = forced_device_index ? (dev_begin + 1) : devices.size();

    for (size_t dev_idx = dev_begin; dev_idx < dev_end; ++dev_idx) {
        const auto &device = devices[dev_idx];
        const std::string device_name = device.getInfo<CL_DEVICE_NAME>();

        int lock_fd = -1;
        if (!is_emulation_mode()) {
            lock_fd = try_acquire_device_lock(dev_idx);
            if (lock_fd < 0) {
                std::ostringstream oss;
                oss << "device[" << dev_idx << "] is busy/in use; skipping";
                device_failures.push_back(oss.str());
                continue;
            }
        }

        std::cout << "Trying device[" << dev_idx << "]: " << device_name
                  << std::endl;

        cl_int err = CL_SUCCESS;

        cl::Context context(device, nullptr, nullptr, nullptr, &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx << "] context creation failed (CL="
                << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        cl::CommandQueue q(context, device, CL_QUEUE_PROFILING_ENABLE, &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx << "] command queue creation failed (CL="
                << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        std::vector<cl::CommandQueue> big_queues;
        big_queues.reserve(acc.num_big_krnl);
        bool failed = false;

        for (int k = 0; k < acc.num_big_krnl; ++k) {
            cl::CommandQueue queue(context, device, CL_QUEUE_PROFILING_ENABLE,
                                   &err);
            if (err != CL_SUCCESS) {
                std::ostringstream oss;
                oss << "device[" << dev_idx
                    << "] big queue creation failed at index " << k
                    << " (CL=" << err << ")";
                device_failures.push_back(oss.str());
                failed = true;
                break;
            }
            big_queues.push_back(queue);
        }
        if (failed) {
            release_device_lock(lock_fd);
            continue;
        }

        std::vector<cl::CommandQueue> little_queues;
        little_queues.reserve(acc.num_little_krnl);
        for (int k = 0; k < acc.num_little_krnl; ++k) {
            cl::CommandQueue queue(context, device, CL_QUEUE_PROFILING_ENABLE,
                                   &err);
            if (err != CL_SUCCESS) {
                std::ostringstream oss;
                oss << "device[" << dev_idx
                    << "] little queue creation failed at index " << k
                    << " (CL=" << err << ")";
                device_failures.push_back(oss.str());
                failed = true;
                break;
            }
            little_queues.push_back(queue);
        }
        if (failed) {
            release_device_lock(lock_fd);
            continue;
        }

        cl::CommandQueue apply_queue(context, device, CL_QUEUE_PROFILING_ENABLE,
                                     &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx
                << "] apply queue creation failed (CL=" << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        cl::CommandQueue hbm_writer_queue(context, device,
                                          CL_QUEUE_PROFILING_ENABLE, &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx
                << "] hbm_writer queue creation failed (CL=" << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        cl::Program program(context, {device}, bins, nullptr, &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx << "] xclbin program failed (CL="
                << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        std::vector<cl::Kernel> big_kernels;
        big_kernels.reserve(acc.num_big_krnl);
        for (int i = 0; i < acc.num_big_krnl; ++i) {
            std::string cu_id = std::to_string(i + 1);
            std::string kernel_name =
                std::string("graphyflow_big:{") + "graphyflow_big_" + cu_id +
                "}";

            cl::Kernel kernel(program, kernel_name.c_str(), &err);
            if (err != CL_SUCCESS) {
                std::ostringstream oss;
                oss << "device[" << dev_idx << "] failed to create "
                    << kernel_name << " (CL=" << err << ")";
                device_failures.push_back(oss.str());
                failed = true;
                break;
            }
            printf("Creating a big kernel [%s] for CU(%d)\n", kernel_name.c_str(),
                   i + 1);
            big_kernels.push_back(kernel);
        }
        if (failed) {
            release_device_lock(lock_fd);
            continue;
        }

        std::vector<cl::Kernel> little_kernels;
        little_kernels.reserve(acc.num_little_krnl);
        for (int i = 0; i < acc.num_little_krnl; ++i) {
            std::string cu_id = std::to_string(i + 1);
            std::string kernel_name = std::string("graphyflow_little:{") +
                                      "graphyflow_little_" + cu_id + "}";

            cl::Kernel kernel(program, kernel_name.c_str(), &err);
            if (err != CL_SUCCESS) {
                std::ostringstream oss;
                oss << "device[" << dev_idx << "] failed to create "
                    << kernel_name << " (CL=" << err << ")";
                device_failures.push_back(oss.str());
                failed = true;
                break;
            }
            printf("Creating a little kernel [%s] for CU(%d)\n",
                   kernel_name.c_str(), i + 1);
            little_kernels.push_back(kernel);
        }
        if (failed) {
            release_device_lock(lock_fd);
            continue;
        }

        cl::Kernel apply_kernel(program, "apply_kernel:{apply_kernel_1}", &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx
                << "] failed to create apply kernel (CL=" << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        cl::Kernel hbm_writer_kernel(program, "hbm_writer:{hbm_writer_1}", &err);
        if (err != CL_SUCCESS) {
            std::ostringstream oss;
            oss << "device[" << dev_idx
                << "] failed to create hbm_writer kernel (CL=" << err << ")";
            device_failures.push_back(oss.str());
            release_device_lock(lock_fd);
            continue;
        }

        acc.context = context;
        acc.q = q;
        acc.big_gs_queue = std::move(big_queues);
        acc.little_gs_queue = std::move(little_queues);
        acc.apply_queue = apply_queue;
        acc.hbm_writer_queue = hbm_writer_queue;

        acc.big_gs_krnls = std::move(big_kernels);
        acc.little_gs_krnls = std::move(little_kernels);
        acc.apply_krnl = apply_kernel;
        acc.hbm_writer_krnl = hbm_writer_kernel;

        acc.selected_device_index = static_cast<int>(dev_idx);
        acc.device_lock_fd = lock_fd;

        std::cout << "Device program successful on device[" << dev_idx << "]"
                  << std::endl;
        return acc;
    }

    std::cerr << "Failed to initialize accelerator on all available devices."
              << std::endl;
    for (const auto &detail : device_failures) {
        std::cerr << "  - " << detail << std::endl;
    }
    std::exit(EXIT_FAILURE);
}
