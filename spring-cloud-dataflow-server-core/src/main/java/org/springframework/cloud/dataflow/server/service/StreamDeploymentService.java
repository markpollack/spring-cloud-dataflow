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
package org.springframework.cloud.dataflow.server.service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import org.springframework.cloud.dataflow.configuration.metadata.ApplicationConfigurationMetadataResolver;
import org.springframework.cloud.dataflow.core.ApplicationType;
import org.springframework.cloud.dataflow.core.BindingPropertyKeys;
import org.springframework.cloud.dataflow.core.DataFlowPropertyKeys;
import org.springframework.cloud.dataflow.core.StreamAppDefinition;
import org.springframework.cloud.dataflow.core.StreamDefinition;
import org.springframework.cloud.dataflow.core.StreamPropertyKeys;
import org.springframework.cloud.dataflow.registry.AppRegistration;
import org.springframework.cloud.dataflow.registry.AppRegistry;
import org.springframework.cloud.dataflow.rest.util.DeploymentPropertiesUtils;
import org.springframework.cloud.dataflow.server.DataFlowServerUtil;
import org.springframework.cloud.dataflow.server.config.apps.CommonApplicationProperties;
import org.springframework.cloud.dataflow.server.controller.WhitelistProperties;
import org.springframework.cloud.dataflow.server.repository.DeploymentIdRepository;
import org.springframework.cloud.dataflow.server.repository.DeploymentKey;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.core.AppDefinition;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.skipper.client.SkipperClient;
import org.springframework.cloud.skipper.client.io.DefaultPackageWriter;
import org.springframework.cloud.skipper.client.io.PackageWriter;
import org.springframework.cloud.skipper.domain.ConfigValues;
import org.springframework.cloud.skipper.domain.InstallProperties;
import org.springframework.cloud.skipper.domain.InstallRequest;
import org.springframework.cloud.skipper.domain.Package;
import org.springframework.cloud.skipper.domain.PackageIdentifier;
import org.springframework.cloud.skipper.domain.PackageMetadata;
import org.springframework.cloud.skipper.domain.Template;
import org.springframework.cloud.skipper.domain.UploadRequest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import static org.springframework.cloud.deployer.spi.app.AppDeployer.COUNT_PROPERTY_KEY;

/**
 * @author Mark Pollack
 */
@Service
public class StreamDeploymentService {

	/**
	 * This is the spring boot property key that Spring Cloud Stream uses to filter the
	 * metrics to import when the specific Spring Cloud Stream "applicaiton" trigger is fired
	 * for metrics export.
	 */
	private static final String METRICS_TRIGGER_INCLUDES = "spring.metrics.export.triggers.application.includes";
	private static final String DEFAULT_PARTITION_KEY_EXPRESSION = "payload";
	private static Log logger = LogFactory.getLog(StreamDeploymentService.class);
	private static String deployLoggingString = "Deploying application named [%s] as part of stream named [%s] "
			+ "with resource URI [%s]";
	private final WhitelistProperties whitelistProperties;
	/**
	 * The app registry this controller will use to lookup apps.
	 */
	private final AppRegistry appRegistry;

	/**
	 * The deployer this controller will use to deploy stream apps.
	 */
	private final AppDeployer appDeployer;

	/**
	 * The repository this controller will use for deployment IDs.
	 */
	private final DeploymentIdRepository deploymentIdRepository;

	private final SkipperClient skipperClient;

	/**
	 * General properties to be applied to applications on deployment.
	 */
	private final CommonApplicationProperties commonApplicationProperties;

	public StreamDeploymentService(AppRegistry appRegistry,
			CommonApplicationProperties commonApplicationProperties,
			ApplicationConfigurationMetadataResolver metadataResolver,
			AppDeployer appDeployer,
			DeploymentIdRepository deploymentIdRepository,
			SkipperClient skipperClient) {
		Assert.notNull(appRegistry, "AppRegistry must not be null");
		Assert.notNull(commonApplicationProperties, "CommonApplicationProperties must not be null");
		Assert.notNull(metadataResolver, "MetadataResolver must not be null");
		Assert.notNull(appDeployer, "AppDeployer must not be null");
		Assert.notNull(deploymentIdRepository, "DeploymentIdRepository must not be null");
		this.appRegistry = appRegistry;
		this.commonApplicationProperties = commonApplicationProperties;
		this.whitelistProperties = new WhitelistProperties(metadataResolver);
		this.appDeployer = appDeployer;
		this.deploymentIdRepository = deploymentIdRepository;
		this.skipperClient = skipperClient;
	}

	public void deploy(StreamDefinition streamDefinition,
			List<Pair<AppDeploymentRequest, StreamAppDefinition>> pairList) {

		if (skipperClient != null) {
			logger.info("Deploying Stream using skipper");
			deployUsingSkipper(streamDefinition, pairList);
		}
		for (Pair<AppDeploymentRequest, StreamAppDefinition> pair : pairList) {
			AppDeploymentRequest appDeploymentRequest = pair.getFirst();
			StreamAppDefinition streamAppDefinition = pair.getSecond();
			try {
				ApplicationType type = DataFlowServerUtil.determineApplicationType(streamAppDefinition);
				AppRegistration registration = this.appRegistry.find(streamAppDefinition.getRegisteredAppName(), type);
				logger.info(String.format(deployLoggingString, appDeploymentRequest.getDefinition().getName(),
						streamAppDefinition.getStreamName(), registration.getUri()));
				String id = this.appDeployer.deploy(appDeploymentRequest);
				this.deploymentIdRepository.save(DeploymentKey.forStreamAppDefinition(streamAppDefinition), id);
			}
			catch (Exception e) {
				logger.error(
						String.format("Exception when deploying the app %s: %s", streamAppDefinition, e.getMessage()),
						e);
			}
		}
	}

	private void deployUsingSkipper(StreamDefinition streamDefinition,
			List<Pair<AppDeploymentRequest, StreamAppDefinition>> pairList) {
		//Create the package .zip file to upload
		File packageFile = createPackageForStream(streamDefinition, pairList);

		//Upload the package
		UploadRequest uploadRequest = new UploadRequest();
		uploadRequest.setName(streamDefinition.getName());
		uploadRequest.setVersion("1.0.0");
		uploadRequest.setExtension("zip");
		uploadRequest.setRepoName("local");
		try {
			uploadRequest.setPackageFileAsBytes(Files.readAllBytes(packageFile.toPath()));
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Can't read packageFile " + packageFile, e);
		}
		skipperClient.upload(uploadRequest);

		//Install the package
		InstallRequest installRequest = new InstallRequest();
		PackageIdentifier packageIdentifier = new PackageIdentifier();
		packageIdentifier.setPackageName(streamDefinition.getName());
		packageIdentifier.setPackageVersion("1.0.0");
		packageIdentifier.setRepositoryName("local");
		installRequest.setPackageIdentifier(packageIdentifier);
		InstallProperties installProperties = new InstallProperties();
		installProperties.setPlatformName("default");
		installProperties.setReleaseName("my" + streamDefinition.getName());
		installProperties.setConfigValues(new ConfigValues());
		installRequest.setInstallProperties(installProperties);
		skipperClient.install(installRequest);

	}

	private File createPackageForStream(StreamDefinition streamDefinition,
			List<Pair<AppDeploymentRequest, StreamAppDefinition>> pairList) {
		PackageWriter packageWriter = new DefaultPackageWriter();
		Package pkgtoWrite = createPackage(streamDefinition, pairList);
		Path tempPath;
		try {
			tempPath = Files.createTempDirectory("streampackages");
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Can't create temp diroectory");
		}
		File outputDirectory = tempPath.toFile();

		File zipFile = packageWriter.write(pkgtoWrite, outputDirectory);
		return zipFile;
	}

	private Package createPackage(StreamDefinition streamDefinition,
			List<Pair<AppDeploymentRequest, StreamAppDefinition>> pairList) {
		Package pkg = new Package();
		PackageMetadata packageMetadata = new PackageMetadata();
		packageMetadata.setName(streamDefinition.getName());
		packageMetadata.setVersion("1.0.0");
		packageMetadata.setMaintainer("dataflow");
		packageMetadata.setDescription(streamDefinition.getDslText());
		pkg.setMetadata(packageMetadata);

		pkg.setDependencies(createDependentPackages(streamDefinition, pairList));

		return pkg;
	}

	private List<Package> createDependentPackages(StreamDefinition streamDefinition,
			List<Pair<AppDeploymentRequest, StreamAppDefinition>> pairList) {
		List<Package> packageList = new ArrayList<>();
		for (Pair<AppDeploymentRequest, StreamAppDefinition> pair : pairList) {
			AppDeploymentRequest appDeploymentRequest = pair.getFirst();
			StreamAppDefinition streamAppDefinition = pair.getSecond();
			packageList.add(createDependentPackage(streamAppDefinition, appDeploymentRequest));
		}
		return packageList;
	}

	private Package createDependentPackage(StreamAppDefinition streamAppDefinition,
			AppDeploymentRequest appDeploymentRequest) {
		Package pkg = new Package();

		PackageMetadata packageMetadata = new PackageMetadata();
		packageMetadata.setName(appDeploymentRequest.getDefinition().getName());
		packageMetadata.setVersion("1.0.0");
		packageMetadata.setMaintainer("dataflow");

		pkg.setMetadata(packageMetadata);

		Map<String, Object> deploymentMap = new HashMap<>();

		try {
			deploymentMap.put("resource", appDeploymentRequest.getResource().getURI().toString());
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Can't get URI of resource", e);
		}
		String countProperty = appDeploymentRequest.getDeploymentProperties().get(COUNT_PROPERTY_KEY);
		int count = (StringUtils.hasText(countProperty)) ? Integer.parseInt(countProperty) : 1;
		deploymentMap.put("count", Integer.toString(count));
		deploymentMap.put("name", appDeploymentRequest.getDefinition().getName());
		deploymentMap.put("applicationProperties", appDeploymentRequest.getDefinition().getProperties());
		deploymentMap.put("deploymentProperties", appDeploymentRequest.getDeploymentProperties());

		ConfigValues configValues = new ConfigValues();
		Map<String, Object> configValueMap = new HashMap<>();
		configValueMap.put("deployment", deploymentMap);

		DumperOptions dumperOptions = new DumperOptions();
		dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
		dumperOptions.setPrettyFlow(true);
		Yaml yaml = new Yaml(dumperOptions);
		configValues.setRaw(yaml.dump(configValueMap));

		pkg.setConfigValues(configValues);
		pkg.setTemplates(createGenericTemplate());
		return pkg;

	}

	public List<Template> createGenericTemplate() {
		Resource resource = new ClassPathResource("/org/springframework/cloud/skipper/client/io/generic-template.yml");
		String genericTempateData = null;
		try {
			genericTempateData = StreamUtils.copyToString(resource.getInputStream(), Charset.defaultCharset());
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Can't load generic template", e);
		}
		Template template = new Template();
		template.setData(genericTempateData);
		try {
			template.setName(resource.getURL().toString());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		List<Template> templateList = new ArrayList<>();
		templateList.add(template);
		return templateList;
	}

	public List<Pair<AppDeploymentRequest, StreamAppDefinition>> createRequests(StreamDefinition stream,
			Map<String, String> streamDeploymentProperties) {
		List<Pair<AppDeploymentRequest, StreamAppDefinition>> pairList = new ArrayList<>();
		if (streamDeploymentProperties == null) {
			streamDeploymentProperties = Collections.emptyMap();
		}
		Iterator<StreamAppDefinition> iterator = stream.getDeploymentOrderIterator();
		int nextAppCount = 0;
		boolean isDownStreamAppPartitioned = false;
		while (iterator.hasNext()) {
			StreamAppDefinition currentApp = iterator.next();
			ApplicationType type = DataFlowServerUtil.determineApplicationType(currentApp);

			AppRegistration registration = this.appRegistry.find(currentApp.getRegisteredAppName(), type);
			Assert.notNull(registration, String.format("no application '%s' of type '%s' exists in the registry",
					currentApp.getName(), type));

			Map<String, String> appDeployTimeProperties = extractAppProperties(currentApp, streamDeploymentProperties);
			Map<String, String> deployerDeploymentProperties = DeploymentPropertiesUtils
					.extractAndQualifyDeployerProperties(streamDeploymentProperties, currentApp.getName());
			deployerDeploymentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, currentApp.getStreamName());

			boolean upstreamAppSupportsPartition = upstreamAppHasPartitionInfo(stream, currentApp,
					streamDeploymentProperties);
			// Set instance count property
			if (deployerDeploymentProperties.containsKey(COUNT_PROPERTY_KEY)) {
				appDeployTimeProperties.put(StreamPropertyKeys.INSTANCE_COUNT,
						deployerDeploymentProperties.get(COUNT_PROPERTY_KEY));
			}
			if (!type.equals(ApplicationType.source)) {
				deployerDeploymentProperties.put(AppDeployer.INDEXED_PROPERTY_KEY, "true");
			}

			// consumer app partition properties
			if (upstreamAppSupportsPartition) {
				updateConsumerPartitionProperties(appDeployTimeProperties);
			}

			// producer app partition properties
			if (isDownStreamAppPartitioned) {
				updateProducerPartitionProperties(appDeployTimeProperties, nextAppCount);
			}

			nextAppCount = getInstanceCount(deployerDeploymentProperties);
			isDownStreamAppPartitioned = isPartitionedConsumer(appDeployTimeProperties, upstreamAppSupportsPartition);

			logger.info(String.format("Downloading resource URI [%s]", registration.getUri()));
			Resource appResource = registration.getResource();
			Resource metadataResource = registration.getMetadataResource();

			// add properties needed for metrics system
			appDeployTimeProperties.put(DataFlowPropertyKeys.STREAM_NAME, currentApp.getStreamName());
			appDeployTimeProperties.put(DataFlowPropertyKeys.STREAM_APP_LABEL, currentApp.getName());
			appDeployTimeProperties.put(DataFlowPropertyKeys.STREAM_APP_TYPE, type.toString());
			StringBuilder sb = new StringBuilder().append(currentApp.getStreamName()).append(".")
					.append(currentApp.getName()).append(".").append("${spring.cloud.application.guid}");
			appDeployTimeProperties.put(StreamPropertyKeys.METRICS_KEY, sb.toString());

			// Merge *definition time* app properties with *deployment time* properties
			// and expand them to their long form if applicable
			AppDefinition revisedDefinition = mergeAndExpandAppProperties(currentApp, metadataResource,
					appDeployTimeProperties);

			AppDeploymentRequest request = new AppDeploymentRequest(revisedDefinition, appResource,
					deployerDeploymentProperties);

			pairList.add(Pair.of(request, currentApp));
		}
		return pairList;
	}

	/**
	 * Extract and return a map of properties for a specific app within the deployment
	 * properties of a stream.
	 *
	 * @param appDefinition the {@link StreamAppDefinition} for which to return a map of
	 * properties
	 * @param streamDeploymentProperties deployment properties for the stream that the app is
	 * defined in
	 * @return map of properties for an app
	 */
	private Map<String, String> extractAppProperties(StreamAppDefinition appDefinition,
			Map<String, String> streamDeploymentProperties) {
		Map<String, String> appDeploymentProperties = new HashMap<>();
		// add common properties first
		appDeploymentProperties.putAll(this.commonApplicationProperties.getStream());
		// add properties with wild card prefix
		String wildCardProducerPropertyPrefix = "app.*.producer.";
		String wildCardConsumerPropertyPrefix = "app.*.consumer.";
		String wildCardPrefix = "app.*.";
		parseAndPopulateProperties(streamDeploymentProperties, appDeploymentProperties, wildCardProducerPropertyPrefix,
				wildCardConsumerPropertyPrefix, wildCardPrefix);
		// add application specific properties
		String producerPropertyPrefix = String.format("app.%s.producer.", appDefinition.getName());
		String consumerPropertyPrefix = String.format("app.%s.consumer.", appDefinition.getName());
		String appPrefix = String.format("app.%s.", appDefinition.getName());
		parseAndPopulateProperties(streamDeploymentProperties, appDeploymentProperties, producerPropertyPrefix,
				consumerPropertyPrefix, appPrefix);
		return appDeploymentProperties;
	}

	/**
	 * Return {@code true} if the upstream app (the app that appears before the provided app)
	 * contains partition related properties.
	 *
	 * @param stream stream for the app
	 * @param currentApp app for which to determine if the upstream app has partition
	 * properties
	 * @param streamDeploymentProperties deployment properties for the stream
	 * @return true if the upstream app has partition properties
	 */
	private boolean upstreamAppHasPartitionInfo(StreamDefinition stream, StreamAppDefinition currentApp,
			Map<String, String> streamDeploymentProperties) {
		Iterator<StreamAppDefinition> iterator = stream.getDeploymentOrderIterator();
		while (iterator.hasNext()) {
			StreamAppDefinition app = iterator.next();
			if (app.equals(currentApp) && iterator.hasNext()) {
				StreamAppDefinition prevApp = iterator.next();
				Map<String, String> appDeploymentProperties = extractAppProperties(prevApp, streamDeploymentProperties);
				return appDeploymentProperties.containsKey(BindingPropertyKeys.OUTPUT_PARTITION_KEY_EXPRESSION)
						|| appDeploymentProperties
								.containsKey(BindingPropertyKeys.OUTPUT_PARTITION_KEY_EXTRACTOR_CLASS);
			}
		}
		return false;
	}

	private void parseAndPopulateProperties(Map<String, String> streamDeploymentProperties,
			Map<String, String> appDeploymentProperties, String producerPropertyPrefix,
			String consumerPropertyPrefix,
			String appPrefix) {
		for (Map.Entry<String, String> entry : streamDeploymentProperties.entrySet()) {
			if (entry.getKey().startsWith(appPrefix)) {
				if (entry.getKey().startsWith(producerPropertyPrefix)) {
					appDeploymentProperties.put(BindingPropertyKeys.OUTPUT_BINDING_KEY_PREFIX
							+ entry.getKey().substring(appPrefix.length()), entry.getValue());
				}
				else if (entry.getKey().startsWith(consumerPropertyPrefix)) {
					appDeploymentProperties.put(
							BindingPropertyKeys.INPUT_BINDING_KEY_PREFIX + entry.getKey().substring(appPrefix.length()),
							entry.getValue());
				}
				else {
					appDeploymentProperties.put(entry.getKey().substring(appPrefix.length()), entry.getValue());
				}
			}
		}
	}

	/**
	 * Return a new app definition where definition-time and deploy-time properties have been
	 * merged and short form parameters have been expanded to their long form (amongst the
	 * whitelisted supported properties of the app) if applicable.
	 */
	/* default */ AppDefinition mergeAndExpandAppProperties(StreamAppDefinition original, Resource metadataResource,
			Map<String, String> appDeployTimeProperties) {
		Map<String, String> merged = new HashMap<>(original.getProperties());
		merged.putAll(appDeployTimeProperties);
		merged = whitelistProperties.qualifyProperties(merged, metadataResource);

		merged.putIfAbsent(StreamPropertyKeys.METRICS_PROPERTIES, "spring.application.name,spring.application.index,"
				+ "spring.cloud.application.*,spring.cloud.dataflow.*");
		merged.putIfAbsent(METRICS_TRIGGER_INCLUDES, "integration**");

		return new AppDefinition(original.getName(), merged);
	}

	/**
	 * Add app properties for producing partitioned data to the provided properties.
	 *
	 * @param properties properties to update
	 * @param nextInstanceCount the number of instances for the next (downstream) app in the
	 * stream
	 */
	private void updateProducerPartitionProperties(Map<String, String> properties, int nextInstanceCount) {
		properties.put(BindingPropertyKeys.OUTPUT_PARTITION_COUNT, String.valueOf(nextInstanceCount));
		if (!properties.containsKey(BindingPropertyKeys.OUTPUT_PARTITION_KEY_EXPRESSION)) {
			properties.put(BindingPropertyKeys.OUTPUT_PARTITION_KEY_EXPRESSION, DEFAULT_PARTITION_KEY_EXPRESSION);
		}
	}

	/**
	 * Add app properties for consuming partitioned data to the provided properties.
	 *
	 * @param properties properties to update
	 */
	private void updateConsumerPartitionProperties(Map<String, String> properties) {
		properties.put(BindingPropertyKeys.INPUT_PARTITIONED, "true");
	}

	/**
	 * Return the app instance count indicated in the provided properties.
	 *
	 * @param properties deployer properties for the app for which to determine the count
	 * @return instance count indicated in the provided properties; if the properties do not
	 * contain a count, a value of {@code 1} is returned
	 */
	private int getInstanceCount(Map<String, String> properties) {
		return Integer.valueOf(properties.getOrDefault(COUNT_PROPERTY_KEY, "1"));
	}

	/**
	 * Return {@code true} if an app is a consumer of partitioned data. This is determined
	 * either by the deployment properties for the app or whether the previous (upstream) app
	 * is publishing partitioned data.
	 *
	 * @param appDeploymentProperties deployment properties for the app
	 * @param upstreamAppSupportsPartition if true, previous (upstream) app in the stream
	 * publishes partitioned data
	 * @return true if the app consumes partitioned data
	 */
	private boolean isPartitionedConsumer(Map<String, String> appDeploymentProperties,
			boolean upstreamAppSupportsPartition) {
		return upstreamAppSupportsPartition
				|| (appDeploymentProperties.containsKey(BindingPropertyKeys.INPUT_PARTITIONED)
						&& appDeploymentProperties.get(BindingPropertyKeys.INPUT_PARTITIONED).equalsIgnoreCase("true"));
	}

}
