from typing import Optional, Dict
from apollo.credentials.base import BaseCredentialsService
from apollo.credentials.env_var import EnvVarCredentialsService
from apollo.credentials.asm import AwsSecretsManagerCredentialsService

SELF_HOSTED_CREDENTIALS_TYPE = "self_hosted_credentials_type"
SELF_HOSTED_CREDENTIALS_ENV_VAR = "env_var"
SELF_HOSTED_CREDENTIALS_ASM = "aws_secrets_manager"
SELF_HOSTED_CREDENTIALS_TYPES = {
    SELF_HOSTED_CREDENTIALS_ENV_VAR: EnvVarCredentialsService,
    SELF_HOSTED_CREDENTIALS_ASM: AwsSecretsManagerCredentialsService,
}


class CredentialsFactory:
    """
    Factory class used to fetch credentials from different sources,
    for example when self-hosted credentials are stored in env vars.
    """

    @staticmethod
    def get_credentials_service(credentials: Dict) -> BaseCredentialsService:
        self_hosted_credentials_type = credentials.get(SELF_HOSTED_CREDENTIALS_TYPE)
        if (
            self_hosted_credentials_type
            and self_hosted_credentials_type in SELF_HOSTED_CREDENTIALS_TYPES
        ):
            return SELF_HOSTED_CREDENTIALS_TYPES[self_hosted_credentials_type]()
        return BaseCredentialsService()
