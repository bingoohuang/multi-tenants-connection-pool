/*
 * Copyright (C) 2013 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.bingoohuang.mtcp;

/**
 * The javax.management MBean for a Light pool instance.
 *
 * @author Brett Wooldridge
 */
public interface LightPoolMXBean {
   int getIdleConnections();

   int getActiveConnections();

   int getTotalConnections();

   int getThreadsAwaitingConnection();

   void softEvictConnections();

   void suspendPool();

   void resumePool();
}