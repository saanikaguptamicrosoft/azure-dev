from azure.identity import DefaultAzureCredential 

from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient 



credential = DefaultAzureCredential() 

client = CognitiveServicesManagementClient(credential, "46de64dd-dcd9-4fa6-ac61-3cc4ffc2a7e0") 

result = client.managed_compute_deployments.get( 

    resource_group_name="jingyizhu-dev", 

    account_name="jingyizhu-tip-ai-services", 

    deployment_name="test-embedding-3-small", 

) 



print(f"Name:               {result.name}") 

print(f"State:              {result.properties.provisioning_state}") 

print(f"Model:              {result.properties.model}") 

print(f"DeploymentTemplate: {result.properties.deployment_template}") 

print(f"AcceleratorType:    {result.properties.accelerator_type}") 

print(f"AccelPerInstance:    {result.properties.accelerators_per_instance}") 

print(f"TotalAccelerators:  {result.properties.total_accelerators}") 

print(f"SKU:                {result.sku.name} (capacity={result.sku.capacity})") 

print(f"Routes:             {result.properties.routes}") 

print(f"VersionUpgrade:     {result.properties.version_upgrade_option}")