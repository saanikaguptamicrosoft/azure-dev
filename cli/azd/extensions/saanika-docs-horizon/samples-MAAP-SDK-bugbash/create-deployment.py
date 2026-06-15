from azure.identity import DefaultAzureCredential 

from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient 

from azure.mgmt.cognitiveservices.models import ( 

    ManagedComputeDeployment, 

    ManagedComputeDeploymentProperties, 

    Sku, 

) 



credential = DefaultAzureCredential() 

client = CognitiveServicesManagementClient(credential, "46de64dd-dcd9-4fa6-ac61-3cc4ffc2a7e0") 



deployment = ManagedComputeDeployment( 

    properties=ManagedComputeDeploymentProperties( 

        model="azureml://registries/azureml-hftest-staging/models/google--gemma-4-31b-it/versions/9", 

        deployment_template="azureml://registries/azureml-hftest-staging/deploymenttemplates/template-a100-16k--google--gemma-4-31b-it/labels/latest", 

        version_upgrade_option="OnceNewDefaultVersionAvailable", 

    ), 

    sku=Sku(name="GlobalManagedCompute", capacity=1), 

) 



poller = client.managed_compute_deployments.begin_create_or_update( 

    resource_group_name="jingyizhu-dev", 

    account_name="jingyizhu-tip-ai-services", 

    deployment_name="my-deployment", 

    resource=deployment, 

) 



print(f"LRO started — status: {poller.status()}") 



# Option A: Don't wait — just confirm it was accepted 

result = client.managed_compute_deployments.get( 

    resource_group_name="jingyizhu-dev", 

    account_name="jingyizhu-tip-ai-services", 

    deployment_name="my-deployment", 

) 

print(f"State: {result.properties.provisioning_state}") 



# Option B: Wait for completion (can take 5-15 minutes) 

import time 

while not poller.done(): 

    print(f"Status: {poller.status()}...") 

    time.sleep(30) 

result = poller.result() 

print(f"Final state: {result.properties.provisioning_state}") 

