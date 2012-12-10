[sqlτyped](https://github.com/jonifreeman/sqltyped) converts SQL string literals into typed functions at compile time.
 
    select age, name from person where age > ?
        
==>

    Int => List[{ age: Int, name: String }]

