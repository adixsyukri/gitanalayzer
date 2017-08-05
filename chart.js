let chart = dc.seriesChart("#chart");
let commits = dc.numberDisplay("#commits");
let line_changes = dc.numberDisplay("#lineChanges");
let repotable = dc.dataTable("#repotable");

let reducer = [
    function(p, v) {
        p.sum += +v.lines_change;
        p.count += 1;
        return p
    },
    function(p, v) {
        p.sum -= +v.lines_change;
        p.count -= 1;
        return p
    },
    function(p, v) {
        return {
            'sum': 0,
            'count': 0
        }
    }
]

function drop_to_zero_group(key_gap, group) {
    return {
        all: function() {
            var _all = group.all(),
                result = [];
            _all.forEach(function(kv) {
                result.push(kv);
                result.push({ key: key_gap(kv.key), value: 0 });
            })
            return result;
        }
    }
}

function increment_date(val) {
    return [val[0], new Date(val[1].getTime() - 1)];
}

d3.json('repo-stats.json', function(data) {
    if (data) {

        let dates_list = []
        let ndx = crossfilter(data);

        let dim = ndx.dimension(function(d) {
            dates_list.push(new Date(d.datetime))
            return [d.repo, new Date(d.datetime)]
        })

        let group = dim.group().reduceSum(function(d) {
            return d.lines_change
        })

        chart.options({
            width: 1300,
            height: 580,
            chart: function(d) {
                return dc.lineChart(d).interpolate('linear').evadeDomainFilter(true);
            },
            x: d3.time.scale().domain([new Date(Math.min.apply(null, dates_list)), new Date(Math.max.apply(null, dates_list))]),
            brushOn: false,
            yAxisLabel: "Number of line changes",
            xAxisLabel: "Time",
            elasticY: true,
            dimension: dim,
            group: drop_to_zero_group(increment_date, group),
            seriesAccessor: function(d) { return d.key[0]; },
            keyAccessor: function(d) { return d.key[1] },
            valueAccessor: function(d) { return d.value },
            legend: dc.legend().x(1100).y(10).itemHeight(13).gap(7).horizontal(1).legendWidth(300).itemWidth(300),
            label: function(d) {
                return d.key + ": " + d.value
            }
        })

        chart.margins().left += 40;
        chart.margins().bottom += 15;
        chart.margins().top += 40;
        chart.margins().right += 150;

        let commitDim = ndx.dimension(function(d) {
            return d
        }).group().reduceCount(function(d) {
            return d
        })

        commits.group(commitDim)
            .formatNumber(d3.format(".g"))
            .valueAccessor(function(d) {
                return d.value;
            })

        let line_changes_dim = ndx.dimension(function(d) {
            return d
        }).group().reduceSum(function(d) {
            return d.lines_change;
        })

        line_changes.group(line_changes_dim)
            .formatNumber(d3.format(".g"))
            .valueAccessor(function(d) {
                return d.value
            })

        let tableDim = ndx.dimension(function(d) {
            return d.repo;
        })

        let groupTableDim = tableDim.group().reduce(
            function(p, v) {
                ++p.count;
                p.sum += +v.lines_change;
                return p;
            },
            function(p, v) {
                --p.count;
                p.sum -= +v.lines_change;
                return p;
            },
            function() {
                return { 'sum': 0, 'count': 0 }
            }
        )

        repotable.dimension(groupTableDim)
            .group(function(d) { return "Repository" })
            .size(Infinity)
            .columns([
                function(d) { return d.key },
                function(d) { return d.value.sum },
                function(d) { return d.value.count }
            ])
            .sortBy(function(d) { return d.value.sum })
            .order(d3.descending);

        /*repotable.options({
            dimension: groupTableDim,
            group: function(d) { return "Repository" },
            size: Infinity,
            columns: [
                function(d) { return d.key },
                function(d) { return d.value.sum },
                function(d) { return d.value.count }
            ],
            sortBy: function(d) { return d.value.sum },
            order: d3.
        })*/

        dc.renderAll();
        var reload = function() {
            setTimeout(function() {
                d3.json('repo-stats.json', function(data2) {
                    ndx.remove();
                    ndx.add(data2);
                    dc.redrawAll();
                    reload();
                })
            }, 5000)
        }

        reload();
    }
})