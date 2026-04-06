/********
 * Copyright (c) 2017, Xilinx®, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1.  Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 * 2.  Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 * 3.  Neither the name of the copyright holder nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 ********/

/*******************************************************************************

    Description: HLS kernel to run_vector_addition in hardware

*******************************************************************************/

#pragma once

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

#include <CL/cl2.hpp>

// When creating a buffer with user pointer, you must specify CL_MEM_USE_HOST_PTR to allow
// OpenCL to access data pointed to by the pointer. Create a buffer with this flag if you want
// to avoid buffer copies between host and device.
template <typename T>
cl::Buffer createBuffer(CL_MEM_FLAGS flags, size_t size, T* host_ptr) {
    cl_int err;
    cl::Buffer buffer(err);
    OCL_CHECK(err, buffer = cl::Buffer(CL_MEM_USE_HOST_PTR | flags, size, host_ptr, &err));
    return buffer;
}

template <typename T>
struct aligned_allocator {
    using value_type = T;
    T* allocate(std::size_t num) {
        void* ptr = nullptr;
        if (posix_memalign(&ptr, 4096, num * sizeof(T))) {
            throw std::bad_alloc();
        }
        return reinterpret_cast<T*>(ptr);
    }
    void deallocate(T* p, std::size_t num) {
        free(p);
    }
};

namespace xcl {

static std::vector<cl::Device> get_xil_devices() {
    size_t i;
    std::vector<cl::Platform> platforms;
    cl::Platform::get(&platforms);
    cl::Platform platform;
    for (i = 0; i < platforms.size(); i++) {
        platform = platforms[i];
        std::string platformName = platform.getInfo<CL_PLATFORM_NAME>();
        if (platformName == "Xilinx") {
            break;
        }
    }
    if (i == platforms.size()) {
        std::cout << "Error: Failed to find Xilinx platform" << std::endl;
        exit(EXIT_FAILURE);
    }
    // Getting ACCELERATOR Devices and selecting 1st such device
    std::vector<cl::Device> devices;
    platform.getDevices(CL_DEVICE_TYPE_ACCELERATOR, &devices);
    return devices;
}

static char* read_binary_file(const std::string& xclbin_file_name, unsigned& nb) {
    std::cout << "INFO: Reading " << xclbin_file_name << std::endl;
    if (access(xclbin_file_name.c_str(), R_OK) != 0) {
        printf("ERROR: %s xclbin not available please build first\n", xclbin_file_name.c_str());
        exit(EXIT_FAILURE);
    }
    // Loading XCL Bin into char buffer
    std::ifstream bin_file(xclbin_file_name, std::ifstream::binary);
    bin_file.seekg(0, bin_file.end);
    nb = bin_file.tellg();
    bin_file.seekg(0, bin_file.beg);
    char* buf = new char[nb];
    bin_file.read(buf, nb);
    return buf;
}

static std::vector<unsigned char> read_binary_file(const std::string& xclbin_file_name) {
    if (access(xclbin_file_name.c_str(), R_OK) != 0) {
        printf("ERROR: %s xclbin not available please build first\n", xclbin_file_name.c_str());
        exit(EXIT_FAILURE);
    }
    std::cout << "INFO: Reading " << xclbin_file_name << std::endl;
    // Loading XCL Bin into char buffer
    std::ifstream bin_file(xclbin_file_name, std::ifstream::binary);

    // Size of the file
    bin_file.seekg(0, bin_file.end);
    auto nb = bin_file.tellg();
    bin_file.seekg(0, bin_file.beg);

    // Read file
    std::vector<unsigned char> buf(nb);
    bin_file.read(reinterpret_cast<char*>(buf.data()), nb);
    return buf;
}

static bool is_emulation() {
    bool ret = false;
    char* env = std::getenv("XCL_EMULATION_MODE");
    if (env != nullptr) {
        ret = true;
    }
    return ret;
}

static bool is_hw_emulation() {
    bool ret = false;
    char* env = std::getenv("XCL_EMULATION_MODE");
    if ((env != nullptr) && !std::strcmp(env, "hw_emu")) {
        ret = true;
    }
    return ret;
}

static std::string getBoardVendor(cl_device_id device) {
    size_t size = 0;
    cl_int err = CL_SUCCESS;
    OCL_CHECK(err, err = clGetDeviceInfo(device, CL_DEVICE_VENDOR, 0, nullptr, &size));
    if (err != CL_SUCCESS) {
        throw std::runtime_error("Unable to get CL_DEVICE_VENDOR size");
    }
    std::unique_ptr<char[]> vendor(new char[size]);
    OCL_CHECK(err, err = clGetDeviceInfo(device, CL_DEVICE_VENDOR, size, vendor.get(), nullptr));
    if (err != CL_SUCCESS) {
        throw std::runtime_error("Unable to get CL_DEVICE_VENDOR");
    }
    return vendor.get();
}

} // namespace xcl
