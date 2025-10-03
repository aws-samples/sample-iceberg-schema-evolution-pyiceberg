from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda, Duration,
    aws_lakeformation as lakeformation,
    RemovalPolicy,
    CfnOutput as output
)
from aws_cdk import aws_iam as iam
from constructs import Construct


class IcebergSchemaEvolutionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account_id = Stack.of(self).account
        resource_prefix = "iceberg-schema-evolution"
        # Create bucket with account number in the name
        bucket = s3.Bucket(
            self,
            f"{self.stack_name}-bucket",
            bucket_name=f"{resource_prefix}-{account_id}",  # e.g., my-bucket-123456789012
            removal_policy=RemovalPolicy.DESTROY,  # Optional: auto-delete bucket on stack destroy
            auto_delete_objects=True,  # Optional: auto-delete objects on stack destroy
        )

        self.lambda_ise_role = iam.Role(
            self,
            f"{self.stack_name}-ise-role",
            role_name=f"{resource_prefix}-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("lambda.amazonaws.com"),
            )
        )

        ise_policy_doc = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=["s3:GetObject",
                             "s3:PutObject",
                             "s3:DeleteObject",
                             "s3:ListBucket",
                             "s3:GetObjectVersion"],
                    resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"],
                ),
                iam.PolicyStatement(
                    actions=["glue:GetDatabase",
                             "glue:GetTable",
                             "glue:GetTables",
                             "glue:CreateDatabase",
                             "glue:CreateTable",
                             "glue:UpdateTable",
                             "glue:DeleteTable",
                             "glue:GetPartition",
                             "glue:GetPartitions",
                             "glue:CreatePartition",
                             "glue:BatchCreatePartition",
                             "glue:UpdatePartition",
                             "glue:DeletePartition"],
                    resources=["*"],
                )
            ]
        )
        # ise_docpolicy = iam.PolicyDocument(
        #     statements=[
        #         iam.PolicyStatement(
        #             actions=["glue:*", "s3:*"],
        #             resources=["*"],
        #         )
        #     ]
        # )

        self.ise_policy = iam.Policy(
            self,
            f"{self.stack_name}-ise-access-policy",
            policy_name=f"{resource_prefix}-policy",
            document=ise_policy_doc,
        )

        self.lambda_ise_role.attach_inline_policy(self.ise_policy)

        self.lambda_ise_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )
        # Create Lambda function from Docker image
        self.iceberg_lambda = _lambda.DockerImageFunction(
            self,
            "IcebergSchemaLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
            ),
            architecture=_lambda.Architecture.X86_64,
            memory_size=512,
            function_name=f"{resource_prefix}-function",
            timeout=Duration.seconds(600),
            role=self.lambda_ise_role,
            environment={
                "DATA_BUCKET": bucket.bucket_name,
            }
        )
        # lf_principal = lakeformation.CfnPrincipalPermissions.DataLakePrincipalProperty(
        #     data_lake_principal_identifier=self.lambda_ise_role.role_arn,
        # )
        # catalog_resource = lakeformation.CfnPrincipalPermissions.ResourceProperty(catalog=lakeformation.CfnPrincipalPermissions.CatalogResourceProperty(
        #             catalog_id=account_id # The AWS account ID is the catalog ID
        #         ))
        # lakeformation.CfnPrincipalPermissions(
        #     self, "CreateDatabasePermission",
        #     principal=lf_principal,
        #     resource=catalog_resource,
        #     permissions=["CREATE_DATABASE"],
        #     permissions_with_grant_option=[] # Set to [] if no grant option is needed
        # )

        self.lambda_output = output(
            self,
            id="IcebergSchemaEvolution-Function",
            value=self.iceberg_lambda.function_arn,
        )
        self.lambda_role = output(
            self,
            id="IcebergSchemaEvolution-Role",
            value=self.lambda_ise_role.role_arn,
        )
        self.s3_bucket = output(
            self,
            id="IcebergSchemaEvolution-Bucket",
            value=bucket.bucket_arn,

        )
