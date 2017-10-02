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
package org.springframework.cloud.dataflow.server.stream;

import java.util.List;

import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.util.Assert;

/**
 * @author Mark Pollack
 */
public class StreamDeploymentRequest {

	/**
	 * Name of stream.
	 */
	private final String name;

	/**
	 * DSL definition for stream.
	 */
	private final String dslText;

	/**
	 * The list of app deployment requests for the stream.
	 */
	private final List<AppDeploymentRequest> appDeploymentRequests;

	public StreamDeploymentRequest(String name, String dslText, List<AppDeploymentRequest> appDeploymentRequests) {
		Assert.hasText(name, "name is required");
		Assert.hasText(dslText, "dslText is required");
		Assert.notNull(appDeploymentRequests, "appDeploymentRequests can not be null");
		this.name = name;
		this.dslText = dslText;
		this.appDeploymentRequests = appDeploymentRequests;
	}

	/**
	 * Return the name of this stream.
	 *
	 * @return stream name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return the DSL definition for this stream.
	 *
	 * @return stream definition DSL
	 */
	public String getDslText() {
		return dslText;
	}

	/**
	 * Return the list of {@link AppDeploymentRequest} for the stream.
	 * @return list of app requests
	 */
	public List<AppDeploymentRequest> getAppDeploymentRequests() {
		return appDeploymentRequests;
	}
}
