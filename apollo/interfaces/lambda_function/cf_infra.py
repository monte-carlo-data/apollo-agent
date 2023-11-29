from typing import Dict

from apollo.agent.infra import AgentInfraProvider
from apollo.interfaces.lambda_function.cf_utils import CloudFormationUtils


class LambdaCFInfraProvider(AgentInfraProvider):
    def get_infra_details(self) -> Dict:
        """
        Returns a dictionary with infrastructure information, containing the following attributes:
        - template: the TemplateBody from the CloudFormation template.
        - parameters: the "Parameters" attribute from the CloudFormation stack details.
        """
        client = CloudFormationUtils.get_cloudformation_client()
        stack_id = CloudFormationUtils.get_stack_id()

        template = client.get_template(StackName=stack_id).get("TemplateBody")
        parameters = CloudFormationUtils.get_stack_parameters(client)
        return {
            "template": template,
            "parameters": parameters,
        }
