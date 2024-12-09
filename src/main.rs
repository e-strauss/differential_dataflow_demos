extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Join;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;

fn main() {
    println!("-----------------------------------------------------------------------------------");
    demo1();
    println!("-----------------------------------------------------------------------------------");
    //demo2();
    println!("-----------------------------------------------------------------------------------");
}

fn demo1() {
    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args(), move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // create a manager
        let probe = worker.dataflow(|scope| {

            // create a new collection from an input session.
            let manages = input.to_collection(scope);

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .map(|(m2, m1)| (m1, m2))
                .join(&manages)
                // .inspect(|x| println!("{:?}", x))
                .probe()
        });

        // Set a size for our organization from the input.
        let size = std::env::args().nth(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);

        // Load input (a binary tree).
        input.advance_to(0);
        let mut person = worker.index();
        while person < size {
            input.insert((person/2, person));
            person += worker.peers();
        }

        // wait for data loading.
        input.advance_to(1);
        input.flush();
        while probe.less_than(&input.time()) { worker.step(); }
        println!("{:?}\tdata loaded", worker.timer().elapsed());

        // make changes, but await completion.
        let mut person = 1 + worker.index();
        while person < size {
            input.remove((person/2, person));
            input.insert((person/3, person));
            input.advance_to(person);
            input.flush();
            while probe.less_than(&input.time()) { worker.step(); }
            println!("{:?}\tstep {} complete", worker.timer().elapsed(), person);
            person += worker.peers();
        }

    }).expect("Computation terminated abnormally");
}

fn demo2() {
    let _ = timely::execute_from_args(std::env::args(), move |worker| {

        let mut probe = Handle::new();
        let (mut customers_input, mut orders_input) = worker.dataflow(|scope| {
            let (customers_input, customers) = scope.new_collection();
            let (orders_input, orders) = scope.new_collection();

            /*
            CREATE VIEW  HighPriceOrdersPerCustomer AS
                 SELECT  Customers.Name, COUNT(*) AS NumOrders
                   FROM  Customers
                   JOIN  Orders ON Customers.Name = Orders.Name
                  WHERE  Orders.Price > 250
               GROUP BY  Customers.Name
            */
            let high_priced_orders_per_customer =
                orders
                    .filter(|(_name, (_category, price))| *price > 250)
                    .join_map(&customers, |name: &String, _, _| (*name).to_string())
                    .inspect(|(record, time, change)| {
                        eprintln!(
                            "\t Customer: {:?}, time: {:?}, change in order count: {:?}",
                            record,
                            time,
                            change
                        )
                    });

            high_priced_orders_per_customer.probe_with(&mut probe);

            (customers_input, orders_input)
        });

        let initial_customers = [
            ("Bob".to_string(),   ("99 High St.".to_string(),   415000)),
            ("Aliya".to_string(), ("125 Baker St.".to_string(), 415202)),
            ("Ji".to_string(),    ("76 Square St.".to_string(), 415123)),
        ];

        let initial_orders = [
            ("Bob".to_string(),   ("Clothing".to_string(),  1200)),
            ("Bob".to_string(),   ("Clothing".to_string(),  500)),
            ("Aliya".to_string(), ("Furniture".to_string(), 300)),
        ];


        customers_input.advance_to(0);
        orders_input.advance_to(0);

        for customer in initial_customers {
            customers_input.insert(customer);
        }

        for order in initial_orders {
            orders_input.insert(order);
        }

        customers_input.close();
        orders_input.advance_to(1);
        orders_input.flush();

        println!("\n\t -- time 0 -> 1 --------------------");
        worker.step_while(|| probe.less_than(orders_input.time()));


        let canceled_orders = [
            ("Bob".to_string(),   ("Clothing".to_string(),  500)),
        ];

        let new_orders = [
            ("Bob".to_string(),   ("Clothing".to_string(),  100)),
            ("Ji".to_string(),    ("Furniture".to_string(), 1000)),
            ("Aliya".to_string(), ("Clothing".to_string(),  50)),
        ];

        for order in canceled_orders {
            orders_input.remove(order);
        }

        for order in new_orders {
            orders_input.insert(order);
        }

        orders_input.advance_to(2);
        orders_input.flush();

        println!("\n\t -- time 1 -> 2 --------------------");
        worker.step_while(|| probe.less_than(orders_input.time()));
    });
}