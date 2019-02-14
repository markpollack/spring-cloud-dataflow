/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.dataflow.server.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.springframework.cloud.dataflow.audit.service.AuditRecordService;
import org.springframework.cloud.dataflow.core.AuditActionType;
import org.springframework.cloud.dataflow.core.AuditOperationType;
import org.springframework.cloud.dataflow.core.Launcher;
import org.springframework.cloud.dataflow.core.TaskDefinition;
import org.springframework.cloud.dataflow.core.TaskDeployment;
import org.springframework.cloud.dataflow.core.TaskManifest;
import org.springframework.cloud.dataflow.rest.util.ArgumentSanitizer;
import org.springframework.cloud.dataflow.rest.util.DeploymentPropertiesUtils;
import org.springframework.cloud.dataflow.server.job.LauncherRepository;
import org.springframework.cloud.dataflow.server.repository.TaskDeploymentRepository;
import org.springframework.cloud.dataflow.server.service.TaskExecutionCreationService;
import org.springframework.cloud.dataflow.server.service.TaskExecutionInfoService;
import org.springframework.cloud.dataflow.server.service.TaskExecutionService;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.core.io.Resource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation of the {@link TaskExecutionService} interface. Provide service
 * methods for Tasks.
 *
 * @author Michael Minella
 * @author Marius Bogoevici
 * @author Glenn Renfro
 * @author Mark Fisher
 * @author Janne Valkealahti
 * @author Gunnar Hillert
 * @author Thomas Risberg
 * @author Ilayaperumal Gopinathan
 * @author Michael Wirth
 * @author David Turanski
 * @author Daniel Serleg
 */
@Transactional
public class DefaultTaskExecutionService implements TaskExecutionService {

	public static final String TASK_DEFINITION_DSL_TEXT = "taskDefinitionDslText";

	public static final String TASK_DEPLOYMENT_PROPERTIES = "taskDeploymentProperties";

	public static final String COMMAND_LINE_ARGS = "commandLineArgs";

	public static final String TASK_PLATFORM_NAME = "spring.cloud.dataflow.task.platformName";

	protected final AuditRecordService auditRecordService;

	/**
	 * Used to launch apps as tasks.
	 */
	private final LauncherRepository launcherRepository;

	private final TaskExecutionCreationService taskExecutionRepositoryService;

	/**
	 * Used to create TaskExecutions.
	 */
	private final TaskRepository taskRepository;

	private final TaskExecutionInfoService taskExecutionInfoService;

	private final TaskDeploymentRepository taskDeploymentRepository;

	private final ArgumentSanitizer argumentSanitizer = new ArgumentSanitizer();

	private final TaskAppDeploymentRequestCreator taskAppDeploymentRequestCreator;

	/**
	 * Initializes the {@link DefaultTaskExecutionService}.
	 *
	 * @param launcherRepository the repository of task launcher used to launch task apps.
	 * @param auditRecordService the audit record service
	 * @param taskRepository the repository to use for accessing and updating task executions
	 * @param taskDeploymentRepository the repository to track task deployment
	 * @param taskExecutionInfoService the service used to setup a task execution
	 * @param taskExecutionRepositoryService the service used to create the task execution
	 */
	public DefaultTaskExecutionService(LauncherRepository launcherRepository,
			AuditRecordService auditRecordService,
			TaskRepository taskRepository,
			TaskExecutionInfoService taskExecutionInfoService,
			TaskDeploymentRepository taskDeploymentRepository,
			TaskExecutionCreationService taskExecutionRepositoryService,
			TaskAppDeploymentRequestCreator taskAppDeploymentRequestCreator) {
		Assert.notNull(launcherRepository, "launcherRepository must not be null");
		Assert.notNull(auditRecordService, "auditRecordService must not be null");
		Assert.notNull(taskExecutionInfoService, "taskDefinitionRetriever must not be null");
		Assert.notNull(taskRepository, "taskRepository must not be null");
		Assert.notNull(taskExecutionInfoService, "taskExecutionInfoService must not be null");
		Assert.notNull(taskDeploymentRepository, "taskDeploymentRepository must not be null");
		Assert.notNull(taskExecutionRepositoryService, "taskExecutionRepositoryService must not be null");
		Assert.notNull(taskAppDeploymentRequestCreator, "taskAppDeploymentRequestCreator must not be null");

		this.launcherRepository = launcherRepository;
		this.auditRecordService = auditRecordService;
		this.taskRepository = taskRepository;
		this.taskExecutionInfoService = taskExecutionInfoService;
		this.taskDeploymentRepository = taskDeploymentRepository;
		this.taskExecutionRepositoryService = taskExecutionRepositoryService;
		this.taskAppDeploymentRequestCreator = taskAppDeploymentRequestCreator;

	}

	@Override
	public long executeTask(String taskName, Map<String, String> taskDeploymentProperties,
			List<String> commandLineArgs) {

		if (taskExecutionInfoService.maxConcurrentExecutionsReached()) {
			throw new IllegalStateException(String.format(
					"The maximum concurrent task executions [%d] is at its limit.",
					taskExecutionInfoService.getMaximumConcurrentTasks()));
		}

		String platformName = taskDeploymentProperties.get(TASK_PLATFORM_NAME);
		if (!StringUtils.hasText(platformName)) {
			platformName = "default";
		}
		// Remove since the key for task platform name will not pass validation for app, deployer,
		// or scheduler prefix
		taskDeploymentProperties.remove(TASK_PLATFORM_NAME);

		DeploymentPropertiesUtils.validateDeploymentProperties(taskDeploymentProperties);

		TaskLauncher taskLauncher = findTaskLauncher(platformName);

		TaskDeployment existingTaskDeployment = taskDeploymentRepository
				.findTopByTaskDefinitionNameOrderByCreatedOnAsc(taskName);
		if (existingTaskDeployment != null) {
			if (!existingTaskDeployment.getPlatformName().equals(platformName)) {
				throw new IllegalStateException(String.format(
						"Task definition [%s] has already been deployed on platfrom [%s].  " +
								"Requested to deploy on platform [%s].",
						taskName, existingTaskDeployment.getPlatformName(), platformName));
			}
		}
		TaskExecutionInformation taskExecutionInformation = taskExecutionInfoService
				.findTaskExecutionInformation(taskName, taskDeploymentProperties);
		TaskExecution taskExecution = taskExecutionRepositoryService.createTaskExecution(taskName);

		AppDeploymentRequest appDeploymentRequest = this.taskAppDeploymentRequestCreator.createRequest(taskExecution,
				taskExecutionInformation, commandLineArgs);

		// Task Manifest
		TaskManifest taskManifest = new TaskManifest();
		String composedTaskDsl = taskExecutionInformation.getTaskDefinition().getProperties().get("graph");
		if (StringUtils.hasText(composedTaskDsl)) {
			taskManifest.setDslText(composedTaskDsl); // this is now empty.
			taskManifest.setTaskDeploymentRequest(appDeploymentRequest);
			List<AppDeploymentRequest> subTaskAppDeploymentRequests = this.taskExecutionInfoService.createRequests(
					taskExecutionInformation.getTaskDefinition().getTaskName(),
					taskManifest.getDslText());
			taskManifest.setSubTaskDeploymentRequests(subTaskAppDeploymentRequests);

			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.addMixIn(Resource.class, ResourceMixin.class);
			objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			try {
				String manifestAsString = objectMapper.writeValueAsString(taskManifest);
				System.out.println(manifestAsString);
				// taskDeployment.setTaskManifestString(manifestAsString);
			}
			catch (JsonProcessingException e) {
				throw new IllegalArgumentException("Could not serialize Task Manifest", e);
			}
		}

		String taskDeploymentId = taskLauncher.launch(appDeploymentRequest);
		if (!StringUtils.hasText(taskDeploymentId)) {
			throw new IllegalStateException("Deployment ID is null for the task:" + taskName);
		}
		this.updateExternalExecutionId(taskExecution.getExecutionId(), taskDeploymentId);

		TaskDeployment taskDeployment = new TaskDeployment();
		taskDeployment.setTaskDeploymentId(taskDeploymentId);
		taskDeployment.setPlatformName(platformName);
		taskDeployment.setTaskDefinitionName(taskName);
		this.taskDeploymentRepository.save(taskDeployment);

		this.auditRecordService.populateAndSaveAuditRecordUsingMapData(
				AuditOperationType.TASK, AuditActionType.DEPLOY,
				taskExecutionInformation.getTaskDefinition().getName(),
				getAudited(taskExecutionInformation.getTaskDefinition(),
						taskExecutionInformation.getTaskDeploymentProperties(),
						appDeploymentRequest.getCommandlineArguments()));

		return taskExecution.getExecutionId();
	}

	private TaskLauncher findTaskLauncher(String platformName) {
		Launcher launcher = this.launcherRepository.findByName(platformName);
		if (launcher == null) {
			List<String> launcherNames = StreamSupport.stream(launcherRepository.findAll().spliterator(), false)
					.map(Launcher::getName)
					.collect(Collectors.toList());
			throw new IllegalStateException(String.format("No Launcher found for the platform named '%s'.  " +
					"Available platform names are %s",
					platformName, launcherNames));
		}
		TaskLauncher taskLauncher = launcher.getTaskLauncher();
		if (taskLauncher == null) {
			throw new IllegalStateException(String.format("No TaskLauncher found for the platform named '%s'",
					platformName));
		}
		return taskLauncher;
	}

	protected void updateExternalExecutionId(long executionId, String taskLaunchId) {
		this.taskRepository.updateExternalExecutionId(executionId, taskLaunchId);
	}

	private Map<String, Object> getAudited(TaskDefinition taskDefinition, Map<String, String> taskDeploymentProperties,
			List<String> commandLineArgs) {
		final Map<String, Object> auditedData = new HashMap<>(3);
		auditedData.put(TASK_DEFINITION_DSL_TEXT, this.argumentSanitizer.sanitizeTaskDsl(taskDefinition));
		auditedData.put(TASK_DEPLOYMENT_PROPERTIES,
				this.argumentSanitizer.sanitizeProperties(taskDeploymentProperties));
		auditedData.put(COMMAND_LINE_ARGS, this.argumentSanitizer.sanitizeArguments(commandLineArgs));
		return auditedData;
	}

}
