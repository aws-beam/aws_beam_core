%%%% @doc
%%%
%%% Allows generating either a get or put presigned s3 url.
%%% This can be used by external clients such as cURL to access the object in question.
%%%
%%% See:
%%%   - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
%%%   - https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html
-module(aws_s3_presigned_url).

-export([ make_presigned_v4_url/5,
          make_presigned_v4_url/6
        ]).

%%====================================================================
%% API
%%====================================================================
-spec make_presigned_v4_url(map(), get | put, integer(), binary(), binary()) -> {ok, binary()}.
make_presigned_v4_url(Client0, Method, ExpireSeconds, Bucket, Key) ->
    make_presigned_v4_url(Client0, Method, ExpireSeconds, Bucket, Key, path).

-spec make_presigned_v4_url(map(), get | put, integer(), binary(), binary(),path|virtual_host) -> {ok, binary()}.
make_presigned_v4_url(Client0, Method, ExpireSeconds, Bucket, Key, Style) ->
    MethodBin = method_to_binary(Method),
    Path = build_path(Client0,Bucket,Key,Style),
    Client = Client0#{service => <<"s3">>},
    SecurityToken = aws_client:token(Client),
    AccessKeyID = aws_client:access_key_id(Client),
    SecretAccessKey = aws_client:secret_access_key(Client),
    Region = aws_client:region(Client),
    Service = aws_client:service(Client),
    Host = build_host(<<"s3">>, Client, Bucket,Style),
    URL = build_url(Host, Path, Client),
    Now = calendar:universal_time(),
    Options0 = [ {ttl, ExpireSeconds}
               , {body_digest, <<"UNSIGNED-PAYLOAD">>}
               , {uri_encode_path, false} %% We already encode in build_path/4
               ],
    Options = case SecurityToken of
                undefined ->
                  Options0;
                _ ->
                  [{session_token, aws_util:encode_uri(SecurityToken)} | Options0]
              end,
    {ok, aws_signature:sign_v4_query_params(AccessKeyID, SecretAccessKey, Region, Service, Now, MethodBin, URL, Options)}.

%%====================================================================
%% Internal functions
%%====================================================================
-spec method_to_binary(atom()) -> binary().
method_to_binary(get)     -> <<"GET">>;
method_to_binary(put)     -> <<"PUT">>.

build_path(#{region := <<"local">>} = _Client,Bucket,Key, path = _Style) ->
    ["/", aws_util:encode_uri(Bucket), "/", aws_util:encode_multi_segment_uri(Key), ""];
build_path(_Client,Bucket,Key, path = _Style) ->
    ["/", aws_util:encode_uri(Bucket), "/", aws_util:encode_multi_segment_uri(Key), ""];
build_path(_Client,_Bucket,Key,virtual_host = _Style) ->
    ["/", aws_util:encode_multi_segment_uri(Key), ""].

%% Mocks are notoriously bad with host-style requests, just skip it and use path-style for anything local
%% At some points once the mocks catch up, we should remove this ugly hacks...
build_host(_EndpointPrefix, #{region := <<"local">>, endpoint := Endpoint}, _Bucket, _Style) ->
    <<Endpoint/binary>>;
build_host(_EndpointPrefix, #{region := <<"local">>}, _Bucket, _Style) ->
    <<"localhost">>;
build_host(EndpointPrefix, #{region := Region, endpoint := Endpoint}, _Bucket, path = _Style) ->
    aws_util:binary_join([EndpointPrefix, Region, Endpoint], <<".">>);
build_host(EndpointPrefix, #{region := Region, endpoint := Endpoint}, Bucket, virtual_host = _Style) ->
    aws_util:binary_join([Bucket, EndpointPrefix, Region, Endpoint], <<".">>).

build_url(Host0, Path0, Client) ->
    Proto = aws_client:proto(Client),
    Path = erlang:iolist_to_binary(Path0),
    Host = erlang:iolist_to_binary(Host0),
    Port = aws_client:port(Client),
    aws_util:binary_join([Proto, <<"://">>, Host, <<":">>, Port, Path], <<"">>).

%%====================================================================
%% Unit tests
%%====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Copied from hackney_url to avoid bringing in all of hackney just for aws_beam_core eunit testing
-record(hackney_url, {
  transport        :: atom(),
  scheme           :: atom(),
  netloc   = <<>>  :: binary(),
  raw_path         :: binary() | undefined,
  path = <<>>      :: binary() | undefined | nil,
  qs = <<>>        :: binary(),
  fragment = <<>>  :: binary(),
  host = ""        :: string(),
  port             :: integer() | undefined,
  user = <<>>      :: binary(),
  password = <<>>  :: binary()
 }).

presigned_url_test() ->
    Client = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                              <<"åöä&#&">>, <<"eu-west-1">>),
    {ok, Url} = aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, <<"bucket">>, <<"key">>),
    HackneyUrl = parse_url(Url),
    ParsedQs = parse_qs(HackneyUrl#hackney_url.qs),
    Credential = proplists:get_value(<<"X-Amz-Credential">>, ParsedQs),
    [AccessKeyId, _ShortDate, Region, Service, Request] = binary:split(Credential, <<"/">>, [global]),
    ?assertEqual(https, HackneyUrl#hackney_url.scheme),
    ?assertEqual(443, HackneyUrl#hackney_url.port),
    ?assertEqual("s3.eu-west-1.amazonaws.com", HackneyUrl#hackney_url.host),
    ?assertEqual(<<"/bucket/key">>, HackneyUrl#hackney_url.path),
    ?assertEqual(7, length(ParsedQs)),
    ?assertEqual(<<"AccessKeyID">>, AccessKeyId),
    ?assertEqual(<<"eu-west-1">>, Region),
    ?assertEqual(<<"s3">>, Service),
    ?assertEqual(<<"aws4_request">>, Request),
    ?assertEqual(<<"AWS4-HMAC-SHA256">>, proplists:get_value(<<"X-Amz-Algorithm">>, ParsedQs)),
    ?assertEqual(<<"3600">>, proplists:get_value(<<"X-Amz-Expires">>, ParsedQs)),
    ?assertEqual(<<"åöä&#&">>, proplists:get_value(<<"X-Amz-Security-Token">>, ParsedQs)),
    ?assertEqual(<<"host">>, proplists:get_value(<<"X-Amz-SignedHeaders">>, ParsedQs)).

presigned_url_local_with_endpoint_test() ->
    Client = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                              <<"åöä&#&">>, <<"local">>),
    {ok, Url} = aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, <<"bucket">>, <<"key">>),
    HackneyUrl = parse_url(Url),
    ParsedQs = parse_qs(HackneyUrl#hackney_url.qs),
    Credential = proplists:get_value(<<"X-Amz-Credential">>, ParsedQs),
    [AccessKeyId, _ShortDate, Region, Service, Request] = binary:split(Credential, <<"/">>, [global]),
    ?assertEqual(https, HackneyUrl#hackney_url.scheme),
    ?assertEqual(443, HackneyUrl#hackney_url.port),
    ?assertEqual("amazonaws.com", HackneyUrl#hackney_url.host),
    ?assertEqual(<<"/bucket/key">>, HackneyUrl#hackney_url.path),
    ?assertEqual(7, length(ParsedQs)),
    ?assertEqual(<<"AccessKeyID">>, AccessKeyId),
    ?assertEqual(<<"local">>, Region),
    ?assertEqual(<<"s3">>, Service),
    ?assertEqual(<<"aws4_request">>, Request),
    ?assertEqual(<<"AWS4-HMAC-SHA256">>, proplists:get_value(<<"X-Amz-Algorithm">>, ParsedQs)),
    ?assertEqual(<<"3600">>, proplists:get_value(<<"X-Amz-Expires">>, ParsedQs)),
    ?assertEqual(<<"åöä&#&">>, proplists:get_value(<<"X-Amz-Security-Token">>, ParsedQs)),
    ?assertEqual(<<"host">>, proplists:get_value(<<"X-Amz-SignedHeaders">>, ParsedQs)).

presigned_url_local_without_endpoint_test() ->
    Client0 = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                              <<"åöä&#&">>, <<"local">>),
    Client = maps:without([endpoint],Client0),
    {ok, Url} = aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, <<"bucket">>, <<"key">>),
    HackneyUrl = parse_url(Url),
    ParsedQs = parse_qs(HackneyUrl#hackney_url.qs),
    Credential = proplists:get_value(<<"X-Amz-Credential">>, ParsedQs),
    [AccessKeyId, _ShortDate, Region, Service, Request] = binary:split(Credential, <<"/">>, [global]),
    ?assertEqual(https, HackneyUrl#hackney_url.scheme),
    ?assertEqual(443, HackneyUrl#hackney_url.port),
    ?assertEqual("localhost", HackneyUrl#hackney_url.host),
    ?assertEqual(<<"/bucket/key">>, HackneyUrl#hackney_url.path),
    ?assertEqual(7, length(ParsedQs)),
    ?assertEqual(<<"AccessKeyID">>, AccessKeyId),
    ?assertEqual(<<"local">>, Region),
    ?assertEqual(<<"s3">>, Service),
    ?assertEqual(<<"aws4_request">>, Request),
    ?assertEqual(<<"AWS4-HMAC-SHA256">>, proplists:get_value(<<"X-Amz-Algorithm">>, ParsedQs)),
    ?assertEqual(<<"3600">>, proplists:get_value(<<"X-Amz-Expires">>, ParsedQs)),
    ?assertEqual(<<"åöä&#&">>, proplists:get_value(<<"X-Amz-Security-Token">>, ParsedQs)),
    ?assertEqual(<<"host">>, proplists:get_value(<<"X-Amz-SignedHeaders">>, ParsedQs)).

presigned_url_local_without_without_bucket_does_not_work_test() ->
    Client = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                              <<"åöä&#&">>, <<"local">>),
    ?assertException (error,function_clause,aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, undefined, <<"key">>)).

presigned_url_path_style_test() ->
    Client = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                              <<"åöä&#&">>, <<"eu-west-1">>),
    {ok, Url} = aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, <<"bucket">>, <<"key">>,path),
    HackneyUrl = parse_url(Url),
    ParsedQs = parse_qs(HackneyUrl#hackney_url.qs),
    Credential = proplists:get_value(<<"X-Amz-Credential">>, ParsedQs),
    [AccessKeyId, _ShortDate, Region, Service, Request] = binary:split(Credential, <<"/">>, [global]),
    ?assertEqual(https, HackneyUrl#hackney_url.scheme),
    ?assertEqual(443, HackneyUrl#hackney_url.port),
    ?assertEqual("s3.eu-west-1.amazonaws.com", HackneyUrl#hackney_url.host),
    ?assertEqual(<<"/bucket/key">>, HackneyUrl#hackney_url.path),
    ?assertEqual(7, length(ParsedQs)),
    ?assertEqual(<<"AccessKeyID">>, AccessKeyId),
    ?assertEqual(<<"eu-west-1">>, Region),
    ?assertEqual(<<"s3">>, Service),
    ?assertEqual(<<"aws4_request">>, Request),
    ?assertEqual(<<"AWS4-HMAC-SHA256">>, proplists:get_value(<<"X-Amz-Algorithm">>, ParsedQs)),
    ?assertEqual(<<"3600">>, proplists:get_value(<<"X-Amz-Expires">>, ParsedQs)),
    ?assertEqual(<<"åöä&#&">>, proplists:get_value(<<"X-Amz-Security-Token">>, ParsedQs)),
    ?assertEqual(<<"host">>, proplists:get_value(<<"X-Amz-SignedHeaders">>, ParsedQs)).

presigned_url_virtual_host_style_test() ->
    Client = aws_client:make_temporary_client(<<"AccessKeyID">>, <<"SecretAccessKey">>,
                                              <<"åöä&#&">>, <<"eu-west-1">>),
    {ok, Url} = aws_s3_presigned_url:make_presigned_v4_url(Client, put, 3600, <<"bucket">>, <<"key">>,virtual_host),
    HackneyUrl = parse_url(Url),
    ParsedQs = parse_qs(HackneyUrl#hackney_url.qs),
    Credential = proplists:get_value(<<"X-Amz-Credential">>, ParsedQs),
    [AccessKeyId, _ShortDate, Region, Service, Request] = binary:split(Credential, <<"/">>, [global]),
    ?assertEqual(https, HackneyUrl#hackney_url.scheme),
    ?assertEqual(443, HackneyUrl#hackney_url.port),
    ?assertEqual("bucket.s3.eu-west-1.amazonaws.com", HackneyUrl#hackney_url.host),
    ?assertEqual(<<"/key">>, HackneyUrl#hackney_url.path),
    ?assertEqual(7, length(ParsedQs)),
    ?assertEqual(<<"AccessKeyID">>, AccessKeyId),
    ?assertEqual(<<"eu-west-1">>, Region),
    ?assertEqual(<<"s3">>, Service),
    ?assertEqual(<<"aws4_request">>, Request),
    ?assertEqual(<<"AWS4-HMAC-SHA256">>, proplists:get_value(<<"X-Amz-Algorithm">>, ParsedQs)),
    ?assertEqual(<<"3600">>, proplists:get_value(<<"X-Amz-Expires">>, ParsedQs)),
    ?assertEqual(<<"åöä&#&">>, proplists:get_value(<<"X-Amz-Security-Token">>, ParsedQs)),
    ?assertEqual(<<"host">>, proplists:get_value(<<"X-Amz-SignedHeaders">>, ParsedQs)).

%%====================================================================
%% Test Internal
%%   Copied from hackney_url.erl to avoid bringing in all of hackney just for aws_beam_core eunit testing
%%====================================================================
%% Parse a query or a form from a binary and return a list of properties.
parse_qs(<<>>) ->
  [];
parse_qs(Bin) ->
  Tokens = binary:split(Bin, <<"&">>, [trim_all, global]),
  [case binary:split(Token, <<"=">>, [trim_all]) of
     [T] ->
       {urldecode(T), true};
     [Name, Value] ->
       {urldecode(Name), urldecode(Value)}
   end || Token <- Tokens].

-spec urldecode(binary()) -> binary().
urldecode(Bin) when is_binary(Bin) ->
  urldecode(Bin, <<>>, crash).

-spec urldecode(binary(), binary(), crash | skip) -> binary().
urldecode(<<$%, H, L, Rest/binary>>, Acc, OnError) ->
  G = unhex(H),
  M = unhex(L),
  if	G =:= error; M =:= error ->
    case OnError of skip -> ok; crash -> erlang:error(badarg) end,
    urldecode(<<H, L, Rest/binary>>, <<Acc/binary, $%>>, OnError);
    true ->
      urldecode(Rest, <<Acc/binary, (G bsl 4 bor M)>>, OnError)
  end;
urldecode(<<$%, Rest/binary>>, Acc, OnError) ->
  case OnError of skip -> ok; crash -> erlang:error(badarg) end,
  urldecode(Rest, <<Acc/binary, $%>>, OnError);
urldecode(<<$+, Rest/binary>>, Acc, OnError) ->
  urldecode(Rest, <<Acc/binary, $ >>, OnError);
urldecode(<<C, Rest/binary>>, Acc, OnError) ->
  urldecode(Rest, <<Acc/binary, C>>, OnError);
urldecode(<<>>, Acc, _OnError) ->
  Acc.

-spec unhex(byte()) -> byte() | error.
unhex(C) when C >= $0, C =< $9 -> C - $0;
unhex(C) when C >= $A, C =< $F -> C - $A + 10;
unhex(C) when C >= $a, C =< $f -> C - $a + 10;
unhex(_) -> error.

parse_url(URL) when is_list(URL) ->
  case unicode:characters_to_binary(URL) of
    URL1 when is_binary(URL1) ->
      parse_url(URL1);
    _ ->
      parse_url(unicode:characters_to_binary(list_to_binary(URL)))
  end;
parse_url(<<"http://", Rest/binary>>) ->
  parse_url(Rest, #hackney_url{transport=hackney_tcp,
    scheme=http});
parse_url(<<"https://", Rest/binary>>) ->
  parse_url(Rest, #hackney_url{transport=hackney_ssl,
    scheme=https});
parse_url(<<"http+unix://", Rest/binary>>) ->
  parse_url(Rest, #hackney_url{transport=hackney_local_tcp, scheme=http_unix});
parse_url(URL) ->
  parse_url(URL, #hackney_url{transport=hackney_tcp, scheme=http}).

parse_url(URL, S) ->
  {URL1, Fragment} =  parse_fragment(URL),
  case binary:split(URL1, <<"/">>) of
    [URL1] ->
      parse_addr1(URL1, S#hackney_url{raw_path = raw_fragment(Fragment),
                                      path = <<>>,
                                      fragment = Fragment});
    [Addr] ->
      Path = <<"/">>,
      parse_addr1(Addr, S#hackney_url{raw_path = << Path/binary, (raw_fragment(Fragment))/binary >>,
                                      path = Path,
                                      fragment = Fragment});
    [Addr, Path] ->
      RawPath =  <<"/", Path/binary, (raw_fragment(Fragment))/binary >>,
      {Path1, Query} = parse_path( << "/", Path/binary >>),
      parse_addr(Addr, S#hackney_url{raw_path = RawPath,
                                     path = Path1,
                                     qs = Query,
                                     fragment = Fragment})
  end.

raw_fragment(<<"">>) -> <<"">>;
raw_fragment(Fragment) -> <<"#", Fragment/binary>>.

parse_addr1(Addr, S) ->
  case binary:split(Addr, <<"?">>) of
    [_Addr] ->
     parse_addr(Addr, S);
    [Addr1, Query] ->
      RawPath = << "?", Query/binary, (S#hackney_url.raw_path)/binary >>,
      parse_addr(Addr1, S#hackney_url{raw_path=RawPath, qs=Query})
  end.

parse_addr(Addr, S) ->
  case binary:split(Addr, <<"@">>) of
    [Addr] ->
      parse_netloc(Addr, S#hackney_url{netloc=Addr});
    [Credentials, Addr1] ->
      case binary:split(Credentials, <<":">>) of
        [User, Password] ->
          parse_netloc(Addr1, S#hackney_url{netloc=Addr1,
            user = urldecode(User),
            password = urldecode(Password)});
        [User] ->
          parse_netloc(Addr1, S#hackney_url{netloc = Addr1,
            user = urldecode(User),
            password = <<>> })
      end

  end.

parse_netloc(<<"[", Rest/binary>>, #hackney_url{transport=Transport}=S) ->
  case binary:split(Rest, <<"]">>, [trim]) of
    [Host] when Transport =:= hackney_tcp ->
      S#hackney_url{host=binary_to_list(Host), port=80};
    [Host] when Transport =:= hackney_ssl ->
      S#hackney_url{host=binary_to_list(Host), port=443};
    [Host, <<":", Port/binary>>] when Port /= <<>> ->
      S#hackney_url{host=binary_to_list(Host),
                    port=list_to_integer(binary_to_list(Port))};
    _ ->
      parse_netloc(Rest, S)
  end;

parse_netloc(Netloc, #hackney_url{transport=Transport}=S) ->
  case binary:split(Netloc, <<":">>, [trim]) of
    [Host] when Transport =:= hackney_tcp ->
      S#hackney_url{host=unicode:characters_to_list((Host)),
                    port=80};
    [Host] when Transport =:= hackney_ssl ->
      S#hackney_url{host=unicode:characters_to_list(Host),
                    port=443};
    [Host] when Transport =:= hackney_local_tcp ->
      S#hackney_url{host=unicode:characters_to_list(urldecode(Host)),
                    port=0};
    [Host, Port] ->
      S#hackney_url{host=unicode:characters_to_list(Host),
                    port=list_to_integer(binary_to_list(Port))}
  end.

parse_path(Path) ->
  case binary:split(Path, <<"?">>) of
    [_Path] ->
      {Path, <<>>};
    [Path1, Query] ->
      {Path1, Query}
  end.

parse_fragment(S) ->
  case binary:split(S, <<"#">>) of
    [_S] ->
      {S, <<>>};
    [S1, F] ->
      {S1, F}
  end.

-endif.
