-module(aws_rds_iam_token).
-export([rds_token_create/4]).

-define(SIGNING_ID, <<"rds-db">>).
-define(EMPTY_PAYLOAD_HASH, <<"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855">>).

%%====================================================================
%% API
%%====================================================================
-spec rds_token_create(aws_client:aws_client(), binary(), non_neg_integer(), binary()) -> {ok, binary()}.
rds_token_create(Client, DBEndpoint, DBPort, DBUser) when is_map(Client) andalso
                                                is_binary(DBEndpoint) andalso
                                                is_integer(DBPort) andalso
                                                is_binary(DBUser) ->
  Method = <<"GET">>,
  QueryParams = [{<<"Action">>, <<"connect">>}, {<<"DBUser">>, DBUser}],
  Endpoint = <<"https://", DBEndpoint/binary, ":", (integer_to_binary(DBPort))/binary>>,
  Url = aws_util:add_query(Endpoint, QueryParams),
  AccessKeyID = aws_client:access_key_id(Client),
  SecurityToken = aws_client:token(Client),
  SecretAccessKey = aws_client:secret_access_key(Client),
  Region = aws_client:region(Client),
  Now = calendar:universal_time(),
  Options0 = [ {ttl, timer:minutes(15) div 1000} %% Time in seconds
             , {body_digest, ?EMPTY_PAYLOAD_HASH}
             , {uri_encode_path, false} %% We already encode in build_path/4
             ],
  Options = case SecurityToken of
    undefined ->
      Options0;
    _ ->
      [{session_token, uri_string:quote(SecurityToken)} | Options0]
  end,
  <<"https://", SignedUrl/binary>> = aws_signature:sign_v4_query_params(AccessKeyID, SecretAccessKey, Region, ?SIGNING_ID, Now, Method, Url, Options),
  {ok, SignedUrl}.

%%====================================================================
%% Unit tests
%%====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

fetch_auth_token_test() ->
  Client = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                            <<"Token">>, <<"eu-west-1">>),
  {ok, Url} = rds_token_create(Client, <<"db_endpoint">>, 5432, <<"db_user">>),
  %% uri_string:parse/1 must have a valid scheme so for the sake of testing, let's prefix it
  UrlPrefixed = <<"https://", Url/binary>>,
  Uri = uri_string:parse(UrlPrefixed),
  ParsedQs = uri_string:dissect_query(maps:get(query, Uri, <<>>)),
  Credential = proplists:get_value(<<"X-Amz-Credential">>, ParsedQs),
  [AccessKeyId, _ShortDate, Region, Service, Request] = binary:split(Credential, <<"/">>, [global]),
  ?assertEqual(5432, maps:get(port, Uri)),
  ?assertEqual(<<"db_endpoint">>, maps:get(host, Uri)),
  ?assertEqual(<<"">>, maps:get(path, Uri)),
  ?assertEqual(9, length(ParsedQs)),
  ?assertEqual(<<"AccessKeyID">>, AccessKeyId),
  ?assertEqual(<<"eu-west-1">>, Region),
  ?assertEqual(<<"rds-db">>, Service),
  ?assertEqual(<<"aws4_request">>, Request),
  ?assertEqual(<<"AWS4-HMAC-SHA256">>, proplists:get_value(<<"X-Amz-Algorithm">>, ParsedQs)),
  ?assertEqual(<<"900">>, proplists:get_value(<<"X-Amz-Expires">>, ParsedQs)),
  ?assertEqual(<<"Token">>, proplists:get_value(<<"X-Amz-Security-Token">>, ParsedQs)),
  ?assertEqual(<<"host">>, proplists:get_value(<<"X-Amz-SignedHeaders">>, ParsedQs)).

-endif.
