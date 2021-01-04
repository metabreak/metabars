use chrono::prelude::*;

#[derive(Debug, PartialEq)]
pub enum Bar {
    // closing value
    Single(f64),
    // closing value and count of empty bars
    WithEmpty(f64, usize),
}

pub trait Sampler {
    /// Returns Some(price) if period has been passed, None otherwise
    fn next(&mut self, dt: NaiveDateTime, value: f64) -> Option<Bar>;

    fn next_bar(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime;
}

macro_rules! sampler {
    ($name:tt) => {
        #[derive(Debug)]
        pub struct $name {
            state: Option<State>,
        }

        impl Default for $name {
            fn default() -> Self {
                Self { state: None }
            }
        }
    };
}

#[derive(Debug)]
struct State {
    next_bar: NaiveDateTime,
    last_value: f64,
}

impl State {
    fn new(next_bar: NaiveDateTime, last_value: f64) -> Self {
        Self {
            next_bar,
            last_value,
        }
    }
}

macro_rules! next {
    () => {
        fn next(&mut self, dt: NaiveDateTime, value: f64) -> Option<Bar> {
            match self.state {
                Some(State {
                    next_bar,
                    last_value,
                }) => {
                    if dt >= next_bar {
                        let mut empty_count = 0;
                        // woohoo!
                        // TODO: TwoHardThings
                        let mut next_bar = self.next_bar(next_bar);
                        while dt >= next_bar {
                            empty_count += 1;
                            next_bar = self.next_bar(next_bar);
                        }

                        self.state = Some(State::new(next_bar, value));

                        if empty_count > 0 {
                            Some(Bar::WithEmpty(last_value, empty_count as usize))
                        } else if empty_count < 0 {
                            panic!("{} is less than previous tick date", dt);
                        } else {
                            Some(Bar::Single(last_value))
                        }
                    } else {
                        self.state = Some(State::new(next_bar, value));
                        None
                    }
                }
                None => {
                    let next_bar = self.next_bar(dt);
                    self.state = Some(State::new(next_bar, value));
                    None
                }
            }
        }
    };
}

macro_rules! Minute {
    ($name: ident, $period: expr) => {
        sampler!($name);

        impl Sampler for $name {
            next!();

            #[allow(clippy::modulo_one)]
            fn next_bar(&self, dt: NaiveDateTime) -> NaiveDateTime {
                dt.date()
                    .and_hms(dt.hour(), 0, 0)
                    .checked_add_signed(chrono::Duration::minutes(
                        (dt.minute() + ($period - dt.minute() % $period)) as i64,
                    ))
                    .unwrap()
            }
        }
    };
}

macro_rules! Hour {
    ($name: ident, $period: expr) => {
        sampler!($name);

        impl Sampler for $name {
            next!();

            #[allow(clippy::modulo_one)]
            fn next_bar(&self, dt: NaiveDateTime) -> NaiveDateTime {
                dt.date()
                    .and_hms(0, 0, 0)
                    .checked_add_signed(chrono::Duration::hours(
                        (dt.hour() + ($period - dt.hour() % $period)) as i64,
                    ))
                    .unwrap()
            }
        }
    };
}

Minute!(M1, 1);
Minute!(M2, 2);
Minute!(M3, 3);
Minute!(M4, 4);
Minute!(M5, 5);
Minute!(M6, 6);
Minute!(M10, 10);
Minute!(M12, 12);
Minute!(M15, 15);
Minute!(M20, 20);
Minute!(M30, 30);

Hour!(H1, 1);
Hour!(H2, 2);
Hour!(H3, 3);
Hour!(H4, 4);
Hour!(H6, 6);
Hour!(H8, 8);
Hour!(H12, 12);

sampler!(D1);
impl Sampler for D1 {
    next!();

    fn next_bar(&self, dt: NaiveDateTime) -> NaiveDateTime {
        dt.date()
            .and_hms(0, 0, 0)
            .checked_add_signed(chrono::Duration::days(1))
            .unwrap()
    }
}

sampler!(W1);
impl Sampler for W1 {
    next!();

    fn next_bar(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime {
        let weekday = dt.weekday();
        dt.date()
            .checked_add_signed(chrono::Duration::days(
                7 - weekday.num_days_from_monday() as i64,
            ))
            .unwrap()
            .and_hms(0, 0, 0)
    }
}

sampler!(MN1);
impl Sampler for MN1 {
    next!();

    fn next_bar(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime {
        let date = dt.date();
        let date = if date.month() == 12 {
            // FIXME: bug with B.C.?
            NaiveDate::from_ymd(date.year() + 1, 1, 1)
        } else {
            NaiveDate::from_ymd(date.year(), date.month() + 1, 1)
        };
        date.and_hms(0, 0, 0)
    }
}

impl dyn Sampler {
    pub fn from_short(short: &str) -> Option<Box<dyn Sampler>> {
        match short {
            "M1" => Some(Box::new(M1::default())),
            "M2" => Some(Box::new(M2::default())),
            "M3" => Some(Box::new(M3::default())),
            "M4" => Some(Box::new(M4::default())),
            "M5" => Some(Box::new(M5::default())),
            "M6" => Some(Box::new(M6::default())),
            "M10" => Some(Box::new(M10::default())),
            "M12" => Some(Box::new(M12::default())),
            "M15" => Some(Box::new(M15::default())),
            "M20" => Some(Box::new(M20::default())),
            "M30" => Some(Box::new(M30::default())),
            "H1" => Some(Box::new(H1::default())),
            "H2" => Some(Box::new(H2::default())),
            "H3" => Some(Box::new(H3::default())),
            "H4" => Some(Box::new(H4::default())),
            "H6" => Some(Box::new(H6::default())),
            "H8" => Some(Box::new(H8::default())),
            "H12" => Some(Box::new(H12::default())),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_m15() {
        let mut sampler = M15::default();
        let res = sampler.next(date("2015-01-01 10:03:00"), 0.);
        assert_eq!(res, None);
        let res = sampler.next(date("2015-01-01 10:04:00"), 4.);
        assert_eq!(res, None);

        // new period start, should return prev period closing value
        let res = sampler.next(date("2015-01-01 10:15:00"), 15.);
        assert_eq!(res, Some(Bar::Single(4.)));

        // 15-30 period hasn't passed, should return last period close value
        let res = sampler.next(date("2015-01-01 10:15:01"), 15.);
        assert_eq!(res, None);
        let res = sampler.next(date("2015-01-01 10:15:02"), 16.);
        assert_eq!(res, None);

        // new period
        let res = sampler.next(date("2015-01-01 10:45:02"), 45.);
        assert_eq!(res, Some(Bar::WithEmpty(16., 1)));
    }

    #[test]
    fn test_h12() {
        let mut sampler = H12::default();
        let res = sampler.next(date("2015-01-01 01:03:00"), 0.);
        assert_eq!(res, None);
        let res = sampler.next(date("2015-01-01 01:04:00"), 4.);
        assert_eq!(res, None);

        // new period start, should return prev period closing value
        let res = sampler.next(date("2015-01-01 12:00:00"), 15.);
        assert_eq!(res, Some(Bar::Single(4.)));

        // 12-24 period hasn't passed, should return last period close value
        let res = sampler.next(date("2015-01-01 13:00:00"), 15.);
        assert_eq!(res, None);

        // new period
        let res = sampler.next(date("2015-01-03 10:45:02"), 45.);
        assert_eq!(res, Some(Bar::WithEmpty(15., 2)));
    }

    #[test]
    fn test_d1() {
        let mut sampler = D1::default();
        let res = sampler.next(date("2015-01-03 10:45:02"), 0.);
        assert_eq!(res, None);

        let res = sampler.next(date("2015-01-04 00:00:00"), 1.);
        assert_eq!(res, Some(Bar::Single(0.)));

        let res = sampler.next(date("2015-01-07 00:00:00"), 2.);
        // 05 and 06 are empty
        assert_eq!(res, Some(Bar::WithEmpty(1., 2)))
    }

    #[test]
    fn test_w1() {
        let mut sampler = W1::default();
        // monday
        let res = sampler.next(date("2020-01-04 10:45:02"), 0.);
        assert_eq!(res, None);

        // tuesday
        let res = sampler.next(date("2020-01-05 00:00:00"), 1.);
        assert_eq!(res, None);

        // The next monday
        let res = sampler.next(date("2020-01-11 00:00:00"), 2.);
        assert_eq!(res, Some(Bar::Single(1.)));

        // Two weeks later, tuesday
        let res = sampler.next(date("2020-01-26 00:00:00"), 3.);
        assert_eq!(res, Some(Bar::WithEmpty(2., 1)));
    }

    #[test]
    fn test_mn1() {
        let mut sampler = MN1::default();
        let res = sampler.next(date("2020-01-01 10:45:02"), 0.);
        assert_eq!(res, None);

        let res = sampler.next(date("2020-01-02 00:00:00"), 1.);
        assert_eq!(res, None);

        let res = sampler.next(date("2020-02-02 00:00:00"), 2.);
        assert_eq!(res, Some(Bar::Single(1.)));

        let res = sampler.next(date("2020-10-26 00:00:00"), 3.);
        assert_eq!(res, Some(Bar::WithEmpty(2., 7)));

        let res = sampler.next(date("2021-01-01 00:00:01"), 3.);
        assert_eq!(res, Some(Bar::WithEmpty(3., 2)));
    }

    fn date(date_str: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S").unwrap()
    }
}
