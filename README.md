# AWS Beam Core
AWS Beam Core is a sub-project of the larger AWS Beam projects.
The aim of this project is to consolidate common functions into one place, eliminating the need to implement the same functionality in multiple places.
Initially, AWS Beam Core will be integrated into AWS-Erlang, with plans to integrate into AWS-Elixir in the future.

### Installation
AWS Beam Core will become a dependency of aws-erlang and aws-elxiir.

If only the core library is necessary, it can also be installed as a dependency via rebar3 using either Git or Hex.

Using rebar3 with Git
Add the following to your rebar.config:

```Erlang
{deps, [
    {aws_beam_core, {git, "https://github.com/aws-beam/aws_beam_core.git", {branch, "main"}}}
]}.
```

Using rebar3 with Hex
Add the following to your rebar.config:

```Erlang
{deps, [
    aws_beam_core
]}.
```

### Usage
##### AWS RDS IAM Token Creation
Support for creating IAM Tokens (more info here) has been added as part of the aws_rds_iam_token module.
This allows for easy creation of RDS/Aurora tokens to be used for IAM based authentication instead of username/password combination.

```erlang
> #{access_key_id := AccessKeyID,
    secret_access_key := SecretAccessKey,
    region := Region,
    token := Token} =
    aws_credentials:get_credentials(),
  aws_client:make_temporary_client(AccessKeyID, SecretAccessKey, Token, Region).
> Client = aws_client:make_temporary_client(AccessKeyID, SecretAccessKey, Token, Region).
[...]
> {ok, Url} = aws_rds_iam_token:rds_token_create(Client, <<"db_endpoint">>, 5432, <<"db_user">>).
[...]
```

This token can subsequently be used to connect to the database over IAM. 

##### AWS S3 Presigned Url
Support for Presigning S3 urls has been added as part of the aws_s3_presigned_url module.
This allows generating either a get or put presigned s3 url,
which can be used by external clients such as cURL to access (get/put) the object in question.
```erlang
> #{access_key_id := AccessKeyID,
    secret_access_key := SecretAccessKey,
    region := Region,
    token := Token} =
    aws_credentials:get_credentials(),
  aws_client:make_temporary_client(AccessKeyID, SecretAccessKey, Token, Region).
> Client = aws_client:make_temporary_client(AccessKeyID, SecretAccessKey, Token, Region).
[...]
> {ok, Url} = aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, <<"bucket">>, <<"key">>)
[...]
```

### Contributing
Contributions are welcome! Please fork the repository and submit a pull request.

### Copyright & License
Copyright (c) 2025 - AWS Beam

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

### Contact
For any questions or issues, please open an issue on the GitHub repository.

