let chart = dc.seriesChart("#chart");
let commits = dc.numberDisplay("#commits");
let line_changes = dc.numberDisplay("#lineChanges");
let repotable = dc.dataTable("#repotable");

let reducer = [
    function(p, v) {
        p.sum += +v.lines_change;
        p.count += +v.count;
        return p
    },
    function(p, v) {
        p.sum -= +v.lines_change;
        p.count -= +v.count;
        return p
    },
    function(p, v) {
        return {
            'sum': 0,
            'count': 0
        }
    }
]

/*function drop_to_zero_group(key_gap, group) {
    return {
        all: function() {
            var _all = group.all(),
                result = [];
            _all.forEach(function(kv) {
                result.push(kv);
                result.push({ key: key_gap(kv.key, "add"), value: 0 });
                result.push({ key: key_gap(kv.key, "minus"), value: 0 });
            })
            return result;
        }
    }
}

function increment_date(val, str) {
    if (str == "add") {
        return [val[0], new Date(val[1].getTime() + 1)];
    } else {
        return [val[0], new Date(val[1].getTime() - 1)];
    }
}*/

function findSumOfRepo(acc, item, index) {
    sum = item.value;
    console.log(acc, item, index)
    if (index > 0) {
        for (i = index; i > 0; i--) {
            if (acc[i - 1].key[0] == item.key[0]) {
                sum = sum + acc[i - 1].value
                break;
            }
        }
    }

    return sum;
}

function createCumulativeGroup(group) {
    /**
     * Aggregate ordered list to produce cumulative sum of its values
     *
     * @param {Array} list 
     * @returns {Array}
     */
    function aggregate(list) {
        return list.reduce((acc, item, index) => {
            sum = findSumOfRepo(acc, item, index)
            acc[index] = {
                key: item.key,
                value: sum
            };

            return acc;
        }, []);
    }

    // We need only limited set of methods to implement:
    // all(), top(n) and dispose() are enought to draw a chart.
    return {
        all() {
            return aggregate(group.all());
        },

        top(n) {
            return aggregate(group.top(Infinity)).splice(0, n);
        },

        dispose() {
            if (group.dispose) {
                group.dispose();
            }
        }
    };
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
            return +d.count
        })
        chart.options({
            width: 1300,
            height: 580,
            chart: function(d) {
                return dc.lineChart(d).interpolate('linear').evadeDomainFilter(true);
            },
            x: d3.time.scale().domain([new Date(Math.min.apply(null, dates_list)), new Date(Math.max.apply(null, dates_list))]),
            brushOn: false,
            yAxisLabel: "Sum of number of commits",
            xAxisLabel: "Time",
            elasticY: true,
            dimension: dim,
            group: createCumulativeGroup(group),
            seriesAccessor: function(d) { return d.key[0]; },
            keyAccessor: function(d) { return d.key[1] },
            valueAccessor: function(d) { return d.value },
            legend: dc.legend().x(1150).y(10).itemHeight(13).gap(7).horizontal(1).legendWidth(300).itemWidth(300),
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
        }).group().reduceSum(function(d) {
            return +d.count
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
                p.count += +v.count;
                p.sum += +v.lines_change;
                return p;
            },
            function(p, v) {
                p.count -= +v.count;
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
                    console.debug("Time: " + new Date())
                    ndx.remove();
                    ndx.add(data2);
                    dc.redrawAll();
                    reload();
                })
            }, 5000)
        }

        //reload();
    }
})