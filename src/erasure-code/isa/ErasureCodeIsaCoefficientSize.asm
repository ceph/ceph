;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Ceph coefficient size selector for ISA-L erasure coding
;  Returns 8 for GFNI-capable CPUs, 32 otherwise
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%include "reg_sizes.asm"

default rel
[bits 64]

section .text

global ec_coefficient_size
ec_coefficient_size:
    endbranch
    push rbx

    ; Check the CPU supports Leaf 7. If not, it can't have GFNI.
    xor eax, eax
    cpuid
    cmp eax, 7
    jl .return_32

    ; Check the OS supports AVX (OSXSAVE) and CPU support (AVX)
    mov eax, 1
    cpuid

    test ecx, FLAG_CPUID1_ECX_OSXSAVE
    jz .return_32

    test ecx, FLAG_CPUID1_ECX_AVX
    jz .return_32

    ; Check XMM and YMM registers are enabled
    xor ecx, ecx
    xgetbv
    mov r8d, eax    ; save for ZMM check later
    and eax, FLAG_XGETBV_EAX_XMM_YMM
    cmp eax, FLAG_XGETBV_EAX_XMM_YMM
    jne .return_32

    ; Get leaf 7 (extended features)
    mov eax, 7
    xor ecx, ecx
    cpuid
    mov edi, ecx
    mov esi, ebx

    ; Check for AVX512 with GFNI (AVX512_G2)
    test r8d, FLAG_XGETBV_EAX_ZMM_OPM
    jz .check_avx2_g2    ; AVX512 needs ZMM register, check for AVX2_G2 instead

    mov ecx, edi
    and ecx, FLAGS_CPUID7_ECX_AVX512_G2
    cmp ecx, FLAGS_CPUID7_ECX_AVX512_G2
    je .return_8    ; AVX512 + GFNI supported
    jmp .check_avx2_g2    ; AVX512 but no GFNI support

.check_avx2_g2:
    test esi, FLAG_CPUID7_EBX_AVX2
    jz .return_32    ; No AVX2 support

    mov ecx, edi
    and ecx, FLAGS_CPUID7_ECX_AVX2_G2
    cmp ecx, FLAGS_CPUID7_ECX_AVX2_G2
    jne .return_32    ; AVX2 but no GFNI support

    jmp .return_8    ; AVX2 + GFNI supported

.return_8:
    mov eax, 8
    pop rbx
    ret

.return_32:
    mov eax, 32
    pop rbx
    ret
