-module(aws_util).

-export([binary_join/2,
         add_query/2,
         add_headers/2,
         encode_query/1,
         encode_uri/1,
         encode_multi_segment_uri/1,
         encode_xml/1,
         decode_xml/1
        ]).

-include_lib("xmerl/include/xmerl.hrl").

%%====================================================================
%% API
%%====================================================================
%% @doc Add querystring to url is there are any parameters in the list
-spec add_query(binary(), [{binary(), any()}]) -> binary().
add_query(Url, Query0) ->
    Uri = uri_string:parse(Url),
    ExistingQs = maps:get(query, Uri, <<>>),
    ExistingQsPairs = uri_string:dissect_query(ExistingQs),
    CombinedQs = ExistingQsPairs ++ Query0,
    NewQuery = uri_string:compose_query(CombinedQs),
    uri_string:recompose(maps:put(query, NewQuery, Uri)).

%% @doc Include additions only if they don't already exist in the provided list.
add_headers([], Headers) ->
  Headers;
add_headers([{Name, _} = Header | Additions], Headers) ->
  case lists:keyfind(Name, 1, Headers) of
    false -> add_headers(Additions, [Header | Headers]);
    _ -> add_headers(Additions, Headers)
  end.

%% @doc Join binary values using the specified separator.
binary_join([], _) -> <<"">>;
binary_join([H|[]], _) -> H;
binary_join(L, Sep) when is_list(Sep)  ->
    binary_join(L, list_to_binary(Sep));
binary_join([H|T], Sep) ->
    binary_join(T, H, Sep).

%% @doc Encode URI taking into account if it contains more than one
%% segment.
encode_multi_segment_uri(Value) ->
    Encoded = [ encode_uri(Segment)
                || Segment <- binary:split(Value, <<"/">>, [global])
              ],
    binary_join(Encoded, <<"/">>).

%% @doc Encode URI into a percent-encoding string.
encode_uri(Value) when is_list(Value) ->
  encode_uri(list_to_binary(Value));
encode_uri(Value) when is_binary(Value) ->
  << (uri_encode_path_byte(Byte)) || <<Byte>> <= Value >>.

-spec uri_encode_path_byte(byte()) -> binary().
uri_encode_path_byte($/) -> <<"/">>;
uri_encode_path_byte(Byte)
    when $0 =< Byte, Byte =< $9;
        $a =< Byte, Byte =< $z;
        $A =< Byte, Byte =< $Z;
        Byte =:= $~;
        Byte =:= $_;
        Byte =:= $-;
        Byte =:= $. ->
    <<Byte>>;
uri_encode_path_byte(Byte) ->
    H = Byte band 16#F0 bsr 4,
    L = Byte band 16#0F,
    <<"%", (hex(H, upper)), (hex(L, upper))>>.

%% @doc Encode the map's key/value pairs as a querystring.
%% The query string must be sorted.
%% The query string for query params that do not contain a value such as "key"
%% should be encoded as "key=".
%% Without this fix, the request will result in a SignatureDoesNotMatch error.
encode_query(QueryL) when is_list(QueryL) ->
  uri_string:compose_query(
    lists:sort(
      lists:map(fun({K, true}) -> {K, ""};
                    ({K, V}) when is_binary(V) -> {K, V};
                    ({K, V}) when is_float(V) -> {K, float_to_binary(V, [short])};
                    ({K, V}) when is_integer(V) -> {K, integer_to_binary(V)}
                end, QueryL)));
encode_query(Map) when is_map(Map) ->
  encode_query(maps:to_list(Map)).

%% @doc Encode an Erlang map as XML
%%
%% All keys must be binaries. Values can be a binary, a list, an
%% integer a float or another nested map.
encode_xml(Map) ->
    Result = lists:map(fun encode_xml_key_value/1, maps:to_list(Map)),
    iolist_to_binary(Result).

%% @doc Decode XML into a map representation
%%
%% When there is more than one element with the same tag name, their
%% values get merged into a list.
%%
%% If the content is only text then a key with the element name and a
%% value with the content is inserted.
%%
%% If the content is a mix between text and child elements, then the
%% elements are processed as described above and all the text parts
%% are merged under the binary `__text' key.
decode_xml(Xml) ->
    %% See: https://elixirforum.com/t/utf-8-issue-with-erlang-xmerl-scan-function/1668/9
    XmlString = erlang:binary_to_list(Xml),
    Opts = [{hook_fun, fun hook_fun/2}],
    {Element, []} = xmerl_scan:string(XmlString, Opts),
    Element.

%%====================================================================
%% Internal functions
%%====================================================================

-spec encode_xml_key_value({binary(), any()}) -> iolist().
encode_xml_key_value({K, V}) when is_binary(K), is_binary(V) ->
  ["<", K, ">", V, "</", K, ">"];
encode_xml_key_value({K, List}) when is_binary(K), is_list(List) ->
  case io_lib:char_list(List) of
    true -> ["<", K, ">", list_to_binary(List), "</", K, ">"];
    false -> [encode_xml_key_value({K, V}) || V <- List]
  end;
encode_xml_key_value({K, V}) when is_binary(K), is_integer(V) ->
  ["<", K, ">", integer_to_binary(V), "</", K, ">"];
encode_xml_key_value({K, V}) when is_binary(K), is_float(V) ->
  ["<", K, ">", float_to_binary(V, [short]), "</", K, ">"];
encode_xml_key_value({K, V}) when is_binary(K), is_map(V) ->
  ["<", K, ">", lists:map(fun encode_xml_key_value/1, maps:to_list(V)), "</", K, ">"].

-define(TEXT, <<"__text">>).

%% @doc Callback hook_fun for xmerl parser
hook_fun(#xmlElement{name = Tag, content = Content} , GlobalState) ->
    Value = case lists:foldr(fun content_to_map/2, none, Content) of
                V = #{?TEXT := Text} ->
                    case string:trim(Text) of
                        <<>>    -> maps:remove(?TEXT, V);
                        Trimmed -> V#{?TEXT => Trimmed}
                    end;
                V -> V
            end,
    {#{atom_to_binary(Tag, utf8) => Value}, GlobalState};
hook_fun(#xmlText{value = Text}, GlobalState) ->
    {unicode:characters_to_binary(Text), GlobalState}.

%% @doc Convert the content of an Xml node into a map.
content_to_map(X, none) ->
    X;
content_to_map(X, Acc) when is_map(X), is_map(Acc) ->
    [{Tag, Value}] = maps:to_list(X),
    case maps:is_key(Tag, Acc) of
        true ->
            UpdateFun = fun(L) when is_list(L) ->
                                [Value | L];
                           (V) -> [Value, V]
                        end,
            maps:update_with(Tag, UpdateFun, Acc);
        false -> maps:merge(Acc, X)
    end;
content_to_map(X, #{?TEXT := Text} = Acc)
  when is_binary(X), is_map(Acc) ->
    Acc#{?TEXT => <<X/binary, Text/binary>>};
content_to_map(X, Acc) when is_binary(X), is_map(Acc) ->
    Acc#{?TEXT => X};
content_to_map(X, Acc) when is_binary(X), is_binary(Acc) ->
    <<X/binary, Acc/binary>>;
content_to_map(X, Acc) when is_map(X), is_binary(Acc) ->
    X#{?TEXT => Acc}.

%% @doc Convert an integer in the 0-16 range to a hexadecimal byte
%% representation.
hex(N, upper) ->
  hex(N, $A);
hex(N, lower) ->
  hex(N, $a);
hex(N, _Char) when N >= 0, N < 10 ->
  N + $0;
hex(N, Char) when N < 16 ->
  N - 10 + Char.
binary_join([], Acc, _) ->
    Acc;
binary_join([H|T], Acc, Sep) ->
    binary_join(T, <<Acc/binary, Sep/binary, H/binary>>, Sep).

%%====================================================================
%% Unit tests
%%====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

add_headers_test() ->
  ?assertEqual([{c, d}, {a, b}], add_headers([{a, b}, {c, d}], [{a, b}])).

%% binary_join/2 joins a list of binary values, separated by a separator
%% character, into a single binary value.
binary_join_test() ->
  Bins = [<<"a">>, <<"b">>, <<"c">>],
  Sep = <<",">>,
  ?assertEqual(binary_join(Bins, Sep), <<"a,b,c">>),
  ?assertEqual(binary_join(Bins, Sep), binary_join(Bins, binary_to_list(Sep))).

%% binary_join/2 correctly joins binary values with a multi-character
%% separator.
binary_join_with_multi_character_separator_test() ->
    ?assertEqual(binary_join([<<"a">>, <<"b">>, <<"c">>], <<", ">>),
                 <<"a, b, c">>).

%% binary_join/2 converts a list containing a single binary into the binary
%% itself.
binary_join_with_single_element_list_test() ->
    ?assertEqual(binary_join([<<"a">>], <<",">>), <<"a">>).

%% binary_join/2 returns an empty binary value when an empty list is
%% provided.
binary_join_with_empty_list_test() ->
    ?assertEqual(binary_join([], <<",">>), <<"">>).

%% decode_xml handles lists correctly by merging values in a list.
decode_xml_lists_test() ->
    ?assertEqual(
       #{ <<"person">> =>
              #{ <<"name">> => <<"foo">>
               , <<"addresses">> => #{<<"address">> => [<<"1">>, <<"2">>]}
               }
        },
       decode_xml(<<"<person>"
                    "  <name>foo</name>"
                    "  <addresses>"
                    "    <address>1</address>"
                    "    <address>2</address>"
                    "  </addresses>"
                    "</person>">>)).

%% decode_xml handles multiple text elments mixed with other elements correctly.
decode_xml_text_test() ->
    ?assertEqual( #{ <<"person">> =>
                         #{ <<"name">> => <<"foo">>
                          , ?TEXT => <<"random">>
                          }
                   }
                , decode_xml(<<"<person>"
                               "  <name>foo</name>"
                               "  random"
                               "</person>">>)
                ),

    ?assertEqual( #{<<"person">> => #{ <<"name">> => <<"foo">>
                                     , <<"age">> => <<"42">>
                                     , ?TEXT => <<"random    text">>
                                     }
                   }
                , decode_xml(<<"<person>"
                               "  <name>foo</name>"
                               "  random"
                               "  <age>42</age>"
                               "  text"
                               "</person>">>)
                ).

decode_utf8_xml_text_test() ->
    ?assertEqual( #{ <<"person">> =>
                         #{ <<"name">> => <<"сергей"/utf8>>
                          , ?TEXT => <<"random">>
                          }
                   }
                , decode_xml(<<"<person>"
                               "  <name>сергей</name>"
                               "  random"
                               "</person>"/utf8>>)
                ).

%% encode_uri correctly encode segment of an URI
encode_uri_test() ->
  Segment = <<"hello world!">>,
  ?assertEqual(<<"hello%20world%21">>, encode_uri(Segment)),
  ?assertEqual(encode_uri(Segment), encode_uri(binary_to_list(Segment))).

encode_uri_parenthesis_test() ->
  Segment = <<"hello world(!)">>,
  ?assertEqual(<<"hello%20world%28%21%29">>, encode_uri(Segment)).

encode_uri_special_chars_test() ->
  Segment = <<"file_!-_.(*)&=;:+ ,?{^}%]>[~<#`|.content">>,
  ?assertEqual(<<"file_%21-_.%28%2A%29%26%3D%3B%3A%2B%20%2C%3F%7B%5E%7D%25%5D%3E%5B~%3C%23%60%7C.content">>,
               encode_uri(Segment)).

%% encode_multi_segment_uri correctly encode each segment of an URI
encode_multi_segment_uri_test() ->
  MultiSegment = <<"hello /world!">>,
  ?assertEqual(<<"hello%20/world%21">>, encode_multi_segment_uri(MultiSegment)).

encode_query_test() ->
  ?assertEqual(<<"float=1.21&int=123&two=2">>,
               encode_query([{<<"two">>, <<"2">>}, {<<"float">>, 1.21}, {<<"int">>, 123}])),
  ?assertEqual(<<"float=1.2&int=123&two=2">>,
               encode_query([{<<"two">>, <<"2">>}, {<<"float">>, 1.20}, {<<"int">>, 123}])),
  Input1 = [{<<"two">>, <<"2">>}, {<<"float">>, 1.21}, {<<"int">>, 123}],
  ?assertEqual(encode_query(Input1), encode_query(maps:from_list(Input1))).

encode_xml_test() ->
  ?assertEqual(<<"<float>1.21</float><int>123</int><two>2</two>">>,
               encode_xml(#{<<"two">> => <<"2">>,
                            <<"float">> => 1.21,
                            <<"int">> => 123})),
  ?assertEqual(<<"<float>1.2</float><int>123</int><two>2</two>">>,
               encode_xml(#{<<"two">> => <<"2">>,
                            <<"float">> => 1.2,
                            <<"int">> => 123})),
  ?assertEqual(<<"<bin>binary</bin><list>list</list>">>,
               encode_xml(#{<<"bin">> => <<"binary">>, <<"list">> => "list"})),
  ?assertEqual(<<"<bin>binary</bin><map><m1>map1_b</m1><n2>1.21</n2></map>">>,
               encode_xml(#{<<"bin">> => <<"binary">>,
                            <<"map">> => #{<<"m1">> => <<"map1_b">>, <<"n2">> => 1.21}})),
  ?assertEqual(<<"<l>l1</l><l>l2</l>">>,
               encode_xml(#{<<"l">> => ["l1", "l2"]})).

encode_query_sorted_test() ->
  Query = [{<<"two">>, <<"2">>}, {<<"one">>, <<"1">>}],
  ?assertEqual(<<"one=1&two=2">>, encode_query(Query)).

-endif.
