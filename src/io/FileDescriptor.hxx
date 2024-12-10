// SPDX-License-Identifier: BSD-2-Clause
// author: Max Kellermann <max.kellermann@gmail.com>

#pragma once

#include <cstddef>
#include <span>
#include <utility>

#include <unistd.h>
#include <sys/types.h>

#ifdef _WIN32
#include <wchar.h>
#endif

class UniqueFileDescriptor;

/**
 * An OO wrapper for a UNIX file descriptor.
 *
 * This class is unmanaged and trivial; for a managed version, see
 * #UniqueFileDescriptor.
 */
class FileDescriptor {
protected:
	int fd;

public:
	[[nodiscard]]
	FileDescriptor() = default;

	[[nodiscard]]
	explicit constexpr FileDescriptor(int _fd) noexcept:fd(_fd) {}

	constexpr bool operator==(FileDescriptor other) const noexcept {
		return fd == other.fd;
	}

	constexpr bool IsDefined() const noexcept {
		return fd >= 0;
	}

#ifndef _WIN32
	/**
	 * Ask the kernel whether this is a valid file descriptor.
	 */
	[[gnu::pure]]
	bool IsValid() const noexcept;

	/**
	 * Ask the kernel whether this is a regular file.
	 */
	[[gnu::pure]]
	bool IsRegularFile() const noexcept;

	/**
	 * Ask the kernel whether this is a pipe.
	 */
	[[gnu::pure]]
	bool IsPipe() const noexcept;

	/**
	 * Ask the kernel whether this is a socket descriptor.
	 */
	[[gnu::pure]]
	bool IsSocket() const noexcept;
#endif

	/**
	 * Returns the file descriptor.  This may only be called if
	 * IsDefined() returns true.
	 */
	constexpr int Get() const noexcept {
		return fd;
	}

	void Set(int _fd) noexcept {
		fd = _fd;
	}

	[[nodiscard]]
	int Steal() noexcept {
		return std::exchange(fd, -1);
	}

	void SetUndefined() noexcept {
		fd = -1;
	}

	[[nodiscard]]
	static constexpr FileDescriptor Undefined() noexcept {
		return FileDescriptor(-1);
	}

#ifdef __linux__
	[[nodiscard]]
	bool Open(FileDescriptor dir, const char *pathname,
		  int flags, mode_t mode=0666) noexcept;
#endif

	[[nodiscard]]
	bool Open(const char *pathname, int flags, mode_t mode=0666) noexcept;

#ifdef _WIN32
	[[nodiscard]]
	bool Open(const wchar_t *pathname, int flags, mode_t mode=0666) noexcept;
#endif

	[[nodiscard]]
	bool OpenReadOnly(const char *pathname) noexcept;

#ifdef __linux__
	[[nodiscard]]
	bool OpenReadOnly(FileDescriptor dir,
			  const char *pathname) noexcept;
#endif

#ifndef _WIN32
	[[nodiscard]]
	bool OpenNonBlocking(const char *pathname) noexcept;
#endif

#ifdef __linux__
	[[nodiscard]]
	static bool CreatePipe(FileDescriptor &r, FileDescriptor &w,
			       int flags) noexcept;
#endif

	[[nodiscard]]
	static bool CreatePipe(FileDescriptor &r, FileDescriptor &w) noexcept;

#ifdef _WIN32
	void EnableCloseOnExec() const noexcept {}
	void DisableCloseOnExec() const noexcept {}
	void SetBinaryMode() const noexcept;
#else
	[[nodiscard]]
	static bool CreatePipeNonBlock(FileDescriptor &r,
				       FileDescriptor &w) noexcept;

	void SetBinaryMode() const noexcept {}

	/**
	 * Enable non-blocking mode on this file descriptor.
	 */
	void SetNonBlocking() const noexcept;

	/**
	 * Enable blocking mode on this file descriptor.
	 */
	void SetBlocking() const noexcept;

	/**
	 * Auto-close this file descriptor when a new program is
	 * executed.
	 */
	void EnableCloseOnExec() const noexcept;

	/**
	 * Do not auto-close this file descriptor when a new program
	 * is executed.
	 */
	void DisableCloseOnExec() const noexcept;

#ifdef __linux__
	/**
	 * Set the capacity of the pipe.
	 *
	 * This method ignores errors.
	 */
	void SetPipeCapacity(unsigned capacity) const noexcept;
#endif

	/**
	 * Duplicate this file descriptor.
	 *
	 * @return the new file descriptor or UniqueFileDescriptor{}
	 * on error
	 */
	[[nodiscard]]
	UniqueFileDescriptor Duplicate() const noexcept;

	/**
	 * Duplicate the file descriptor onto the given file descriptor.
	 */
	[[nodiscard]]
	bool Duplicate(FileDescriptor new_fd) const noexcept {
		return ::dup2(Get(), new_fd.Get()) != -1;
	}

	/**
	 * Similar to Duplicate(), but if destination and source file
	 * descriptor are equal, clear the close-on-exec flag.  Use
	 * this method to inject file descriptors into a new child
	 * process, to be used by a newly executed program.
	 */
	bool CheckDuplicate(FileDescriptor new_fd) const noexcept;
#endif

	/**
	 * Close the file descriptor.  It should not be called on an
	 * "undefined" object.  After this call, IsDefined() is guaranteed
	 * to return false, and this object may be reused.
	 */
	bool Close() noexcept {
		return ::close(Steal()) == 0;
	}

	/**
	 * Rewind the pointer to the beginning of the file.
	 */
	[[nodiscard]]
	bool Rewind() const noexcept;

	[[nodiscard]]
	off_t Seek(off_t offset) const noexcept {
		return lseek(Get(), offset, SEEK_SET);
	}

	[[nodiscard]]
	off_t Skip(off_t offset) const noexcept {
		return lseek(Get(), offset, SEEK_CUR);
	}

	[[gnu::pure]]
	off_t Tell() const noexcept {
		return lseek(Get(), 0, SEEK_CUR);
	}

	/**
	 * Returns the size of the file in bytes, or -1 on error.
	 */
	[[gnu::pure]]
	off_t GetSize() const noexcept;

#ifndef _WIN32
	[[nodiscard]]
	ssize_t ReadAt(off_t offset, std::span<std::byte> dest) const noexcept {
		return ::pread(fd, dest.data(), dest.size(), offset);
	}
#endif

	[[nodiscard]]
	ssize_t Read(std::span<std::byte> dest) const noexcept {
		return ::read(fd, dest.data(), dest.size());
	}

	[[nodiscard]]
	ssize_t Read(void *buffer, std::size_t length) const noexcept {
		return ::read(fd, buffer, length);
	}

	/**
	 * Read until all of the given buffer has been filled.  Throws
	 * on error.
	 */
	void FullRead(std::span<std::byte> dest) const;

#ifndef _WIN32
	[[nodiscard]]
	ssize_t WriteAt(off_t offset, std::span<const std::byte> src) const noexcept {
		return ::pwrite(fd, src.data(), src.size(), offset);
	}
#endif

	[[nodiscard]]
	ssize_t Write(std::span<const std::byte> src) const noexcept {
		return ::write(fd, src.data(), src.size());
	}

	[[nodiscard]]
	ssize_t Write(const void *buffer, std::size_t length) const noexcept {
		return ::write(fd, buffer, length);
	}

	/**
	 * Write until all of the given buffer has been written.
	 * Throws on error.
	 */
	void FullWrite(std::span<const std::byte> src) const;

#ifndef _WIN32
	[[nodiscard]]
	int Poll(short events, int timeout) const noexcept;

	[[nodiscard]]
	int WaitReadable(int timeout) const noexcept;

	[[nodiscard]]
	int WaitWritable(int timeout) const noexcept;

	[[gnu::pure]]
	bool IsReadyForWriting() const noexcept;
#endif
};
