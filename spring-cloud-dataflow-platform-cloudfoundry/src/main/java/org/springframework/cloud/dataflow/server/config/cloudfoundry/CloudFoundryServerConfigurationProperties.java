/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.dataflow.server.config.cloudfoundry;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.springframework.boot.autoconfigure.condition.ConditionalOnCloudPlatform;
import org.springframework.boot.cloud.CloudPlatform;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * {@link ConfigurationProperties} class to configure various settings of Data Flow
 * running on Cloud Foundry.
 *
 * @author Eric Bottard
 */
@ConditionalOnCloudPlatform(CloudPlatform.CLOUD_FOUNDRY)
@Validated
@ConfigurationProperties(CloudFoundryServerConfigurationProperties.PREFIX)
public class CloudFoundryServerConfigurationProperties {

	public static final String PREFIX = "spring.cloud.dataflow.server.cloudfoundry";

	/**
	 * Whether to turn on reactor style stacktraces.
	 */
	public boolean debugReactor = false;
	int maxWaitTime = 30000;
	/**
	 * The target percentag of free disk space to always aim for when cleaning downloaded
	 * resources (typically via the local maven repository). Specify as an integer greater
	 * than zero and less than 100. Default is 25.
	 */
	private int freeDiskSpacePercentage = 25;
	private int maxPoolSize = 10;

	@Min(0)
	@Max(100)
	public int getFreeDiskSpacePercentage() {
		return freeDiskSpacePercentage;
	}

	public void setFreeDiskSpacePercentage(int freeDiskSpacePercentage) {
		this.freeDiskSpacePercentage = freeDiskSpacePercentage;
	}

	public boolean isDebugReactor() {
		return debugReactor;
	}

	public void setDebugReactor(boolean debugReactor) {
		this.debugReactor = debugReactor;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public int getMaxWaitTime() {
		return maxWaitTime;
	}

	public void setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

}
