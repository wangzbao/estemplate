package com.yolo.estemplate.controller;

import com.yolo.estemplate.domain.User;
import com.yolo.estemplate.util.PageUtil;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@RestController
@RequestMapping("es")
public class ESController {
    @Autowired(required = false)
    private ElasticsearchTemplate elasticsearchTemplate;

    @GetMapping
    public void getAll() {
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(new MatchAllQueryBuilder())
                .build();
        List<User> users = elasticsearchTemplate.queryForList(searchQuery, User.class);
//        SearchHits<User> search = elasticsearchTemplate.search(searchQuery, User.class);
        System.out.println("总共有" + users.size() + "个。");
        for (User user : users) {
            System.out.println(user);
        }
        System.out.println("-----------");
        List<User> collect = users.stream().filter(user -> !StringUtils.isEmpty(user.getCity())).collect(Collectors.toList());
        for (User user : collect) {
            System.out.println(user);
        }

    }

    @GetMapping("add")
    public void add() {
        User user = new User();
        user.setUser("test2");
        user.setCity("test2");
        user.setCountry("");
        user.setProvince("test2");
        user.setUid(6);
        IndexQuery indexQuery = new IndexQueryBuilder()
                .withObject(user)
                .build();
        String index = elasticsearchTemplate.index(indexQuery);
    }

    @GetMapping("update")
    public void update() {
        Map<String, Object> map = new HashMap<>();
        map.put("country", "1,3");
        map.put("uid", 3);
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.doc(map);

        UpdateQuery updateQuery = new UpdateQueryBuilder()
                .withId("7")
                .withClass(User.class)
                .withUpdateRequest(updateRequest)
                .build();
        UpdateResponse update = elasticsearchTemplate.update(updateQuery);
        System.out.println("是否已经更新：" + update.getResult());
    }

    @GetMapping("bulkUpdate")
    public void bulkUpdate() {
        List<UpdateQuery> list = new ArrayList<>();
        List<String> idlist = Arrays.asList("Np6ZonoB82AEkjTDgDuw", "N56aonoB82AEkjTDuDsy", "1", "2", "3", "4", "7");
        StringBuffer sc = null;
        UpdateRequest updateRequest = new UpdateRequest();
        sc = new StringBuffer("if (ctx._source.country == '1') ctx._source.country = ''");
        Script script = new Script(sc.toString());
        updateRequest.script(script);
        for (String id : idlist) {
            UpdateQuery updateQuery = new UpdateQueryBuilder()
                    .withClass(User.class)
                    .withUpdateRequest(updateRequest)
                    .withId(id)
                    .build();
            list.add(updateQuery);
        }

        try {
            elasticsearchTemplate.bulkUpdate(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("client")
    public void clent() {
        StringBuffer sc = null;
        UpdateRequest updateRequest = new UpdateRequest();
        sc = new StringBuffer("if (ctx._source.country == '') ctx._source.country = '1'");
        Script script = new Script(sc.toString());
        updateRequest.script(script);

        UpdateByQueryRequestBuilder updateByQueryRequestBuilder = UpdateByQueryAction.INSTANCE.newRequestBuilder(elasticsearchTemplate.getClient());
        BulkByScrollResponse bulkByScrollResponse = updateByQueryRequestBuilder
                .source("twitter")
                .filter(termQuery("province", "sichuan"))
                .script(script)
                .get();
        System.out.println("是否已更新：" + bulkByScrollResponse.getUpdated());
        System.out.println(bulkByScrollResponse.toString());
    }

    @GetMapping("sku")
    public void skuDistributed() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date start = df.parse("2021-07-02");
        Date end = df.parse("2021-07-04");
        String starts = df.format(start);
//        String ends = df.format(new DateTime(end).withMillisOfDay(0).plusDays(1).minusMillis(1).toDate());
        String ends = df.format(end);
        System.out.println(start);
        System.out.println(end);

        //时间范围查询
        RangeQueryBuilder dtRange = QueryBuilders
                .rangeQuery("dt")
                .gte(starts)
                .lte(ends)
                .format("yyyy-MM-dd");
        //计划id下的一批任务id，或者单个的任务id，或者一批任务id

        //周期id
        //botid
        //构造数组，初始化需要匹配的值
        String[] taskArray = new String[]{"chengdu", "test1", "cd"};
        List<String> cityList = Arrays.asList("chengdu", "test1", "cd");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .filter(dtRange)
//                .filter(QueryBuilders.termsQuery("city", cityList))
//                .must(QueryBuilders.wildcardQuery("city", "*"));
                // 判断是否为空，当在es总存储的是""的时候失效
//                .must(QueryBuilders.existsQuery("city"));
                .must(QueryBuilders.rangeQuery("city").gt(0));
//                .must(termQuery("uid", "5"));

        NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
                .withQuery(boolQueryBuilder)
                .withPageable(PageRequest.of(0, 5000))
                .withSort(SortBuilders.fieldSort("country").order(SortOrder.DESC))
                .build();

        ScoredPage<User> scroll = (ScoredPage<User>) elasticsearchTemplate.startScroll(10, nativeSearchQuery, User.class);
        List<User> content = scroll.getContent();
//        for (User user : content) {
//            for (String sku : user.getCountry().split(",")) {
//                if (map.containsKey(sku)) {
//                    map.put(sku, (Integer) map.get(sku) + 1);
//                } else {
//                    map.put(sku, 1);
//            }
//            System.out.println(user);
//        }
        for (User user : scroll) {
            System.out.println(user);
        }
        System.out.println("===========");
        HashMap<Object, Object> map = new HashMap<>();

        HashMap<Object, Object> skuRefOrder = new HashMap<>();

        List list = new ArrayList();


        content.stream().forEach(user -> {
//            skuDemo.setskuNames()
            list.addAll(Arrays.asList(user.getCountry().split(",")));

        });

        list.stream().forEach(sku -> {
            if (map.containsKey(sku)) {
                map.put(sku, (Integer) map.get(sku) + 1);
            } else {
                map.put(sku, 1);
            }
        });

        System.out.println(map);

        for (Map.Entry<Object, Object> entry : map.entrySet()) {

            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());

        }
    }

    @GetMapping("time")
    public void time(Date start, Date end) {
        System.out.println(start.getTime());
        System.out.println(end.getTime());
    }

    public static void main(String[] args) throws ParseException {
//        System.out.println(new DateTime("2021-07-02").withMillisOfDay(0).toDate().getTime());
//        System.out.println(new DateTime("2021-07-06").withMillisOfDay(0).plusDays(1).minusMillis(1).toDate().getTime());
//        Date startDate = DateTime.parse("2021-07-02").toDate();
//        Date endDate = DateTime.parse("2021-07-06").toDate();
//        System.out.println(startDate);
//        System.out.println(endDate);
//        System.out.println(startDate.getTime());
//        System.out.println(endDate.getTime());
        ArrayList<Object> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);
        list.add(8);
        list.add(9);
        list.add(10);
        list.add(11);
        List list1 = PageUtil.startPage(list, 2, 10);
        System.out.println(list1);
    }

}
