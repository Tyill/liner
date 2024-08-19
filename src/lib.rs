mod topic;

pub fn create_topic(name: &str)-> topic::Topic{

    return topic::Topic::new(name)
}  
