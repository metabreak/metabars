use chrono::prelude::*;
use rust_decimal::prelude::*;

#[derive(Debug)]
struct State {
    next_bar: NaiveDateTime,
    last_value: Decimal,
}

impl State {
    fn new(next_bar: NaiveDateTime, last_value: Decimal) -> Self {
        Self {
            next_bar,
            last_value,
        }
    }
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

#[derive(Debug, PartialEq)]
pub enum Bar {
    // closing value
    Single(Decimal),
    // closing value and count of empty bars
    WithEmpty(Decimal, usize),
}

macro_rules! next {
    () => {
        fn next(&mut self, dt: NaiveDateTime, value: Decimal) -> Option<Bar> {
            match self.state {
                Some(State {
                    next_bar,
                    last_value,
                }) => {
                    if dt >= next_bar {
                        let mut empty_count = 0;
                        // woohoo!
                        // TODO: TwoHardThings
                        let mut next_bar = Self::next_bar(next_bar);
                        while dt >= next_bar {
                            empty_count += 1;
                            next_bar = Self::next_bar(next_bar);
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
                    let next_bar = Self::next_bar(dt);
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
            fn next_bar(dt: NaiveDateTime) -> NaiveDateTime {
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
            fn next_bar(dt: NaiveDateTime) -> NaiveDateTime {
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

pub trait Sampler {
    /// Returns Some(price) if period has been passed, None otherwise
    fn next(&mut self, dt: NaiveDateTime, value: Decimal) -> Option<Bar>;

    fn next_bar(dt: NaiveDateTime) -> chrono::NaiveDateTime;
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

    fn next_bar(dt: NaiveDateTime) -> NaiveDateTime {
        dt.date()
            .and_hms(0, 0, 0)
            .checked_add_signed(chrono::Duration::days(1))
            .unwrap()
    }
}

sampler!(W1);
impl Sampler for W1 {
    next!();

    fn next_bar(dt: NaiveDateTime) -> chrono::NaiveDateTime {
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

    fn next_bar(dt: NaiveDateTime) -> chrono::NaiveDateTime {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_m15() {
        let mut sampler = M15::default();
        let res = sampler.next(date("2015-01-01 10:03:00"), Decimal::zero());
        assert_eq!(res, None);
        let res = sampler.next(date("2015-01-01 10:04:00"), Decimal::new(4, 0));
        assert_eq!(res, None);

        // new period start, should return prev period closing value
        let res = sampler.next(date("2015-01-01 10:15:00"), Decimal::new(15, 0));
        assert_eq!(res, Some(Bar::Single(Decimal::new(4, 0))));

        // 15-30 period hasn't passed, should return last period close value
        let res = sampler.next(date("2015-01-01 10:15:01"), Decimal::new(15, 0));
        assert_eq!(res, None);
        let res = sampler.next(date("2015-01-01 10:15:02"), Decimal::new(16, 0));
        assert_eq!(res, None);

        // new period
        let res = sampler.next(date("2015-01-01 10:45:02"), Decimal::new(45, 0));
        assert_eq!(res, Some(Bar::WithEmpty(Decimal::new(16, 0), 1)));
    }

    #[test]
    fn test_h12() {
        let mut sampler = H12::default();
        let res = sampler.next(date("2015-01-01 01:03:00"), Decimal::zero());
        assert_eq!(res, None);
        let res = sampler.next(date("2015-01-01 01:04:00"), Decimal::new(4, 0));
        assert_eq!(res, None);

        // new period start, should return prev period closing value
        let res = sampler.next(date("2015-01-01 12:00:00"), Decimal::new(15, 0));
        assert_eq!(res, Some(Bar::Single(Decimal::new(4, 0))));

        // 12-24 period hasn't passed, should return last period close value
        let res = sampler.next(date("2015-01-01 13:00:00"), Decimal::new(15, 0));
        assert_eq!(res, None);

        // new period
        let res = sampler.next(date("2015-01-03 10:45:02"), Decimal::new(45, 0));
        assert_eq!(res, Some(Bar::WithEmpty(Decimal::new(15, 0), 2)));
    }

    #[test]
    fn test_d1() {
        let mut sampler = D1::default();
        let res = sampler.next(date("2015-01-03 10:45:02"), Decimal::zero());
        assert_eq!(res, None);

        let res = sampler.next(date("2015-01-04 00:00:00"), Decimal::new(1, 0));
        assert_eq!(res, Some(Bar::Single(Decimal::zero())));

        let res = sampler.next(date("2015-01-07 00:00:00"), Decimal::new(2, 0));
        // 05 and 06 are empty
        assert_eq!(res, Some(Bar::WithEmpty(Decimal::new(1, 0), 2)))
    }

    #[test]
    fn test_w1() {
        let mut sampler = W1::default();
        // monday
        let res = sampler.next(date("2020-01-04 10:45:02"), Decimal::zero());
        assert_eq!(res, None);

        // tuesday
        let res = sampler.next(date("2020-01-05 00:00:00"), Decimal::new(1, 0));
        assert_eq!(res, None);

        // The next monday
        let res = sampler.next(date("2020-01-11 00:00:00"), Decimal::new(2, 0));
        assert_eq!(res, Some(Bar::Single(Decimal::new(1, 0))));

        // Two weeks later, tuesday
        let res = sampler.next(date("2020-01-26 00:00:00"), Decimal::new(3, 0));
        assert_eq!(res, Some(Bar::WithEmpty(Decimal::new(2, 0), 1)));
    }

    #[test]
    fn test_mn1() {
        let mut sampler = MN1::default();
        let res = sampler.next(date("2020-01-01 10:45:02"), Decimal::zero());
        assert_eq!(res, None);

        let res = sampler.next(date("2020-01-02 00:00:00"), Decimal::new(1, 0));
        assert_eq!(res, None);

        let res = sampler.next(date("2020-02-02 00:00:00"), Decimal::new(2, 0));
        assert_eq!(res, Some(Bar::Single(Decimal::new(1, 0))));

        let res = sampler.next(date("2020-10-26 00:00:00"), Decimal::new(3, 0));
        assert_eq!(res, Some(Bar::WithEmpty(Decimal::new(2, 0), 7)));

        let res = sampler.next(date("2021-01-01 00:00:01"), Decimal::new(3, 0));
        assert_eq!(res, Some(Bar::WithEmpty(Decimal::new(3, 0), 2)));
    }

    fn date(date_str: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S").unwrap()
    }
}
