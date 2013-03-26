/*
 * Copyright (C) 2010 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "JniConstants.h"
#include "ScopedLocalRef.h"

#include <stdlib.h>

jclass JniConstants::inet6AddressClass;
jclass JniConstants::inetAddressClass;
jclass JniConstants::inetSocketAddressClass;
jclass JniConstants::stringClass;

static jclass findClass(JNIEnv* env, const char* name) {
    ScopedLocalRef<jclass> localClass(env, env->FindClass(name));
    jclass result = reinterpret_cast<jclass>(env->NewGlobalRef(localClass.get()));
    if (result == NULL) {
        fprintf(stderr, "failed to find class '%s'", name);
        abort();
    }
    return result;
}

void JniConstants::init(JNIEnv* env) {
    inet6AddressClass = findClass(env, "java/net/Inet6Address");
    inetAddressClass = findClass(env, "java/net/InetAddress");
    inetSocketAddressClass = findClass(env, "java/net/InetSocketAddress");
    stringClass = findClass(env, "java/lang/String");
}
