extern crate ct_nlp_diesel;
extern crate diesel;

use self::diesel::prelude::*;
use self::ct_nlp_diesel::*;

fn main() {
    use ct_nlp_diesel::schema::topic::dsl::*;

    let connection = establish_connection();
}
