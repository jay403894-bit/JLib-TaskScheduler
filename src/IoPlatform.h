// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

#include "../include/IoReactor.h"

#if defined(_WIN32)
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <errno.h>
  #include <netinet/in.h>   
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <unistd.h>
#endif

namespace JLib { namespace ioplat {

#if defined(_WIN32)
    inline constexpr std::int32_t kErrMsgSize = WSAEMSGSIZE;
#else
    inline constexpr std::int32_t kErrMsgSize = EMSGSIZE;
#endif

inline void CloseSocket(IoSocket s) noexcept {
#if defined(_WIN32)
    ::closesocket(static_cast<SOCKET>(s));
#else
    ::close(static_cast<int>(s));
#endif
}

inline bool QuerySocketTriple(IoSocket listener, int& family, int& type, int& proto) noexcept {
#if defined(_WIN32)
    WSAPROTOCOL_INFOW info{};
    int len = sizeof info;
    if (::getsockopt(static_cast<SOCKET>(listener), SOL_SOCKET, SO_PROTOCOL_INFOW,
                     reinterpret_cast<char*>(&info), &len) != 0)
        return false;
    family = info.iAddressFamily;
    type   = info.iSocketType;
    proto  = info.iProtocol;
    return true;
#else
    sockaddr_storage ss{};
    socklen_t sslen = sizeof ss;
    if (::getsockname(static_cast<int>(listener), reinterpret_cast<sockaddr*>(&ss), &sslen) != 0)
        return false;
    family = ss.ss_family;

    socklen_t tlen = sizeof type;
    if (::getsockopt(static_cast<int>(listener), SOL_SOCKET, SO_TYPE, &type, &tlen) != 0)
        return false;

    proto = 0;      
#if defined(SO_PROTOCOL)
    socklen_t plen = sizeof proto;
    (void)::getsockopt(static_cast<int>(listener), SOL_SOCKET, SO_PROTOCOL, &proto, &plen);
#endif
    return true;
#endif
}

inline IoSocket MakeSocket(int family, int type, int proto) noexcept {
#if defined(_WIN32)
    
    const SOCKET s = ::WSASocketW(family, type, proto, nullptr, 0, WSA_FLAG_OVERLAPPED);
    return (s == INVALID_SOCKET) ? 0 : static_cast<IoSocket>(s);
#else
    const int fd = ::socket(family, type, proto);
    return (fd < 0) ? 0 : static_cast<IoSocket>(fd);
#endif
}

inline constexpr IoSocket kNoSocket = 0;

bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) noexcept;

}} 
