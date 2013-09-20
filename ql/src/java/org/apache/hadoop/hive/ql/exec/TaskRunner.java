/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TaskRunner implementation.
 **/

public class TaskRunner extends Thread {
  protected Task<? extends Serializable> tsk;
  protected TaskResult result;
  protected SessionState ss;

  static final private Log LOG = LogFactory.getLog(TaskRunner.class.getName());

  public TaskRunner(Task<? extends Serializable> tsk, TaskResult result) {
    this.tsk = tsk;
    this.result = result;
    ss = SessionState.get();
  }

  public Task<? extends Serializable> getTask() {
    return tsk;
  }

  @Override
  public void run() {
    SessionState.start(ss);
    runSequential();
  }

  /**
   * Launches a task, and sets its exit value in the result variable.
   */

  public void runSequential() {
    LOG.info("55 in TaskRunner.java");
    int exitVal = -101;
    try {
      LOG.info("58 in TaskRunner.java");
      exitVal = tsk.executeTask();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    LOG.info("63 in TaskRunner.java");
    result.setExitVal(exitVal);
    LOG.info("65 in TaskRunner.java");
  }

}
