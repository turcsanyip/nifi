/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.nar;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Aspect
public class LoadNativeLibAspect {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Around("call(void System.load(String)) || call(void Runtime.load(String))")
    public void around(ProceedingJoinPoint joinPoint) throws Throwable {
        String origLibPathStr = (String) joinPoint.getArgs()[0];

        Path origLibPath = Paths.get(origLibPathStr);
        String libFileName = origLibPath.getFileName().toString();

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String prefix = cl.getClass().getName() + "@" + cl.hashCode() + "_";
        String suffix = "_" + libFileName;

        Path tempLibPath = Files.createTempFile(prefix, suffix);
        Files.copy(origLibPath, tempLibPath, REPLACE_EXISTING);

        logger.info("Loading native library via absolute path (original lib: {}, copied lib: {}", origLibPath, tempLibPath);

        joinPoint.proceed(new Object[]{tempLibPath.toString()});
    }
}
