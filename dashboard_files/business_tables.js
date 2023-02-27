
const CDN_URL = "https://cdn.jsdelivr.net/gh/zacharyrgarrett/gifted-key-metrics/"

async function render_business_overview_table(){
    var table = new Tabulator("#business-overview-table", {
        height:"400px",
        layout:"fitColumns",
        columns:[
        {title:"BusinessName", field:"BusinessName"},
        {title:"BrandReview", field:"BrandReview", hozAlign:"center", formatter: "star"},
        {title:"ProductReview", field:"ProductReview", hozAlign:"center", formatter: "star"},
        {title:"DealCount", field:"DealCount", hozAlign:"center", sorter: "number"},
        {title:"RecommendedPercentage", field:"RecommendedPercentage", hozAlign:"center"},
        ],
    });

    const path = CDN_URL + "business_data/business_summarized_all_time.json";
    let response = await fetch(path);
    let business_summarized = await response.json();

    table.setData(business_summarized)
    table.setSort("DealCount", "desc")

    table.on("rowClick", function(e, row){
        business_name = row.getData()["BusinessName"];
        iframe_obj = document.getElementById("business-deal-count-figure");
        iframe_obj.src = "./figures/business_specific/deal_count_figures/" + business_name + ".html"
    });
}
