/* get raw data, tranforming */
conn=new Mongo();
db=conn.getDB("video");
db.getCollection('videos').aggregate([
 {$project:{event:{$split: ["$events", ","] }, DateTime:1, VideoTitle:1} },
 {$unwind:"$event"},
 {$match:{event:'206'}},
 { $project : { dims : { $split: ["$VideoTitle", "|"] }, DateTime:1, event:1} },
 {$project:{sizeofdim:{$size:"$dims"}, dims:1, DateTime:1,event:1}},
 {$match:{sizeofdim:{$gt:1}}},
 {$project:{DateTime:1,pors:{$first:"$dims"}, videoTitle:{$last:"$dims"}}},
 {$out:"middle"} 
    
])

db.getCollection('middle').find({}).forEach(function(doc){
    
    if (doc.pors=="iPhone"){
        
        db.middle.update(
        {_id: doc._id},
        {$set:{platform:doc.pors, site:"NA"}}
        )
    } 
  
    if (doc.pors=="Android Phone"){
        
        db.middle.update(
        {_id: doc._id},
        {$set:{platform:doc.pors, site:"NA"}}
        )
        
        }
     else{
         
         db.middle.update(
        {_id:doc._id},
        {$set:{platform:"Desktop", site:doc.pors}}
        )
    }
})

db.getCollection('middle').aggregate([

{$project:{DateTime:{$toDate:"$DateTime"}, videoTitle:1, platform:1,site:1, processingTime:"$$NOW"}},
{$out:"factTransform"}
])


/* Dimension collections */
db.createCollection("D_Datetime");

db.D_Platform.insert([
{skey:-1, platform:"unknown"}

]);

db.D_Site.insert([
{skey:-1, site:"unknown"}

])

db.D_Title.insert([
{skey:-1, title:"unknown"}

])

/* update dimension table D_Datetime*/
db.getCollection('factTransform').find().forEach(function(doc){
    
    db.D_Datetime.insert(
    
    {datetime:doc.DateTime}
    
    )
    
       
});
/* get dimension Delta table for videoTitle, platform and site */
db.getCollection('factTransform').aggregate([

{$group:{_id:null, coming:{$addToSet:"$videoTitle"}}},
{$unwind:"$coming"},
{$project:{_id:"$coming"}},
{$out:"videoTitle_DL"}

])

db.getCollection('factTransform').aggregate([

{$group:{_id:null, coming:{$addToSet:"$platform"}}},
{$unwind:"$coming"},
{$project:{_id:"$coming"}},
{$out:"platform_DL"}

])

db.getCollection('factTransform').aggregate([

{$group:{_id:null, coming:{$addToSet:"$site"}}},
{$unwind:"$coming"},
{$project:{_id:"$coming"}},
{$out:"site_DL"}

])

/* update dimension table*/
db.getCollection('site_DL').find().forEach(function(doc){
    oldsites=db.D_Site.distinct("site");
    m=0
    for (i=0; i<oldsites.length;i++){
        if (oldsites[i]==doc._id){
            
            m=m+1
            
        }
        }
    
    if (m==0){
        
        db.D_Site.insert({
            skey:db.D_Site.distinct("site").length,
            site:doc._id
            
            })
        
        }
    
    
})


db.getCollection('platform_DL').find().forEach(function(doc){
    oldplatforms=db.D_Platform.distinct("platform");
    m=0
    for (i=0; i<oldplatforms.length;i++){
        if (oldplatforms[i]==doc._id){
            
            m=m+1
            
        }
        }
    if (m==0){
        
        db.D_Platform.insert({
            skey:db.D_Platform.distinct("platform").length,
            platform:doc._id
            
            })
        
        }
    
    
})


db.getCollection('videoTitle_DL').find().forEach(function(doc){
    oldtitles=db.D_Title.distinct("title");
    m=0
    for (i=0; i<oldtitles.length;i++){
        if (oldtitles[i]==doc._id){
            
            m=m+1
            
        }
        }
    if (m==0){
        
        db.D_Title.insert({
            skey:db.D_Title.distinct("title").length,
            title:doc._id
            
              })
          }
 })

/* update fact table */
 db.getCollection('factTransform').aggregate([
{
   $lookup:
     {
       from: "D_Site",
       localField:"site",
       foreignField:"site",
       as: "site_J"
     }
        
},
{
   $lookup:
     {
       from: "D_Platform",
       localField:"platform",
       foreignField:"platform",
       as: "platform_J"
     }
        
},
{
   $lookup:
     {
       from: "D_Title",
       localField:"videoTitle",
       foreignField:"title",
       as: "title_J"
     }
        
},
{$unwind:"$title_J"},
{$unwind:"$site_J"},
{$unwind:"$platform_J"},
{$project:{title_skey:"$title_J.skey", platform_skey:"$platform_J.skey", site_skey:"$site_J.skey", DateTime:1, processingTime:1}},
{$out:"fact"}    
    
])

 db.createCollection("Fact_Persistent");
 db.getCollection('fact').find().forEach(function(doc){
    db.Fact_Persistent.insert(doc)
    
    
})
/* clean middle tables */
db.getCollection('middle').drop()
db.getCollection('site_DL').drop()
db.getCollection('platform_DL').drop()
db.getCollection('videoTitle_DL').drop()
db.getCollection('videos').renameCollection("video_coming_at_" + Date.now())
db.getCollection('factTransform').drop()
db.getCollection('fact').drop()