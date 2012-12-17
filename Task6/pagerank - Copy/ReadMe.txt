There are two way to read Top 100 titles.
1. use   sortjob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
2. use FloatWritable(pagerank) as the kay to sort it.
The result is listed in ascending order.  
Then use ReadFromEnd.java to read bottom-up.


We can use "template.java" to form a template.
The template is Loop invariant in the iterations.