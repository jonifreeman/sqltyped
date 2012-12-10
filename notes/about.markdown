[sqlτyped](https://github.com/jonifreeman/sqltyped) converts SQL string literals into typed functions at compile time.
 
```sql
select age, name from person where age > ?
```
        
  ==>

```scala
Int => List[{ age: Int, name: String }]
```
