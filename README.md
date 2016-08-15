INSTALL:

    $ npm install nodesql-db

INITIALIZE:
app.js ->

     var db = require('nodesql-db')

use a connection string:

    # using coded connection string
    $  node app.js
app.js ->

        var password =  ''; // or 'my_password_is_aight'
        var connStr = "postgresql://postgres:"+password+"@127.0.0.1:5432/mydb";
        // or var connStr = "mysql://root:"+password+"@127.0.0.1:3306/mydb";
        var mydb = new db(connStr);



    /* using a env variables */
        var mydb = new db(); // init function looks for envs by default, then connection string.




SCHEMA TIPS FOR EASIER BETTER DML COMPATIBILITY:

    create table <tableName> (
        <tableName>_id SERIAL PRIMARY KEY,
        ...
    );
    /* replace <tableName> with your table */
    Insert, Update, Upsert, Delete Return the primary key of the table

# Insert
    INSERT INTO user (fname,lname) VALUES ('joe','johns');
    mydb.user.insert().values({ fname: 'joe', lname: 'johns' }).run(console.log);
# Upsert
    /* UPDATE, IF NOT EXISTS, INSERT, IF DUPE then SELECT and RETURN PRIMARY KEY ID. */
    mydb.user.util().upsertUsingColumnValues({ fname: 'joe', lname: 'johns' },{ fname: 'joe', lname: 'johns' },console.log);
# Select
psql Select Example #1->

    mydb# SELECT * FROM user;

Javascript Select Example #1->

    mydb.user.selectAll().run(function(err,data){
        if(err) throw err;
        console.log(data);  
    });

psql Select Example #2->

    SELECT phone_id FROM user
    JOIN phone ON phone.phone_id = user.phone_id
    WHERE user.user_id = 1
    GROUP BY user.user_id, user.time_created
    ORDER BY user.time_created DESC
    OFFSET 2
    LIMIT 5;

# Join , Order By , Group By, Limit , Offset    
Javascript Select Example #2->

    mydb.user.select(['phone_id'])
        .join({ phone: { on: [{"phone.phone_id":"user.phone_id"}] } })
        .where({ user_id: 1 })
        .groupBy("user.user_id, user.time_created")
        .orderBy("user.time_created DESC") // "col1 dir1, col2 dir2,...,etc"
        .offset(2)
        .limit('all') // 1 , 2, 100, 'all'
        .run()
        .then(function(data){
            console.log(data)
        })
        .fail(function(err){
            console.error(err)
        })
        .done()


# Update

    psql ->
        UPDATE user SET fname = 'joe', lname = 'johns' WHERE fname = 'joes';
    js ->
        mydb.user.update().set({ fname: 'joe', lname: 'johns' }).where({ fname: 'joes' }).run(console.log)
# Delete
    psql ->
        DELETE FROM user WHERE true;
    js ->
        mydb.user.deleteFrom().where("true").run(console.log); // oh no! nukes user table.



# Raw PSQL
    mydb.user.rawsql("Select * from user order by random() limit 1").run(console.log);