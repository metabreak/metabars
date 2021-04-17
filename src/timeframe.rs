use chrono::prelude::*;

#[derive(Debug, PartialEq)]
pub struct Bar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub stop_dt: NaiveDateTime,
}

#[derive(Debug, PartialEq)]
pub enum Bars {
    // closing value
    Single(Bar),
    // closing value and count of empty bars
    WithEmpty(Bar, Vec<Bar>),
}

pub trait Sampler: Send {
    /// Returns Some(price) if period has been passed, None otherwise
    fn next_bar(&mut self, dt: NaiveDateTime, value: f64) -> Option<Bars>;

    fn next_bar_dt(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime;
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
    next_bar_dt: NaiveDateTime,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

impl State {
    fn new(next_bar_dt: NaiveDateTime, open: f64, high: f64, low: f64, close: f64) -> Self {
        Self {
            next_bar_dt,
            open,
            high,
            low,
            close,
        }
    }
}

macro_rules! next {
    () => {
        fn next_bar(&mut self, dt: NaiveDateTime, value: f64) -> Option<Bars> {
            match self.state {
                Some(State {
                    next_bar_dt,
                    open,
                    high,
                    low,
                    close,
                }) => {
                    if dt >= next_bar_dt {
                        let full_bar = Bar {
                            open,
                            high,
                            low,
                            close,
                            stop_dt: next_bar_dt,
                        };

                        // TODO: TwoHardThings
                        // woohoo!
                        let mut next_bar_dt = self.next_bar_dt(next_bar_dt);

                        let mut empty_bars = vec![];
                        while dt >= next_bar_dt {
                            empty_bars.push(Bar {
                                open: close,
                                high: close,
                                low: close,
                                close: close,
                                stop_dt: next_bar_dt,
                            });
                            next_bar_dt = self.next_bar_dt(next_bar_dt);
                        }

                        self.state = Some(State::new(next_bar_dt, value, value, value, value));

                        if empty_bars.len() > 0 {
                            Some(Bars::WithEmpty(full_bar, empty_bars))
                        } else {
                            Some(Bars::Single(full_bar))
                        }
                    } else {
                        let high = f64::max(value, high);
                        let low = f64::min(value, low);
                        let close = value;

                        self.state = Some(State::new(next_bar_dt, open, high, low, close));
                        None
                    }
                }
                None => {
                    let next_bar_dt = self.next_bar_dt(dt);
                    self.state = Some(State::new(next_bar_dt, value, value, value, value));
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
            fn next_bar_dt(&self, dt: NaiveDateTime) -> NaiveDateTime {
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
            fn next_bar_dt(&self, dt: NaiveDateTime) -> NaiveDateTime {
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

    fn next_bar_dt(&self, dt: NaiveDateTime) -> NaiveDateTime {
        dt.date()
            .and_hms(0, 0, 0)
            .checked_add_signed(chrono::Duration::days(1))
            .unwrap()
    }
}

sampler!(W1);
impl Sampler for W1 {
    next!();

    fn next_bar_dt(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime {
        let weekday = dt.weekday();
        let sub = weekday.num_days_from_monday() as i64;
        let add = 7 - sub;
        // println!("weekday is {}, sub is {}, add is {}", weekday, sub, add);
        dt.date()
            .checked_add_signed(chrono::Duration::days(add))
            .unwrap()
            .and_hms(0, 0, 0)
    }
}

sampler!(MN1);
impl Sampler for MN1 {
    next!();

    fn next_bar_dt(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime {
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
        let res = sampler.next_bar(date("2015-01-01 10:03:00"), 0.);
        assert_eq!(res, None);
        let res = sampler.next_bar(date("2015-01-01 10:04:00"), 4.);
        assert_eq!(res, None);

        // new period start, should return prev period closing value
        let res = sampler.next_bar(date("2015-01-01 10:15:00"), 15.);
        assert_eq!(
            res,
            Some(Bars::Single(Bar {
                open: 0.,
                high: 4.,
                low: 0.,
                close: 4.,
                stop_dt: date("2015-01-01 10:15:00")
            }))
        );

        // 15-30 period hasn't passed, should return last period close value
        let res = sampler.next_bar(date("2015-01-01 10:15:01"), 16.);
        assert_eq!(res, None);
        let res = sampler.next_bar(date("2015-01-01 10:15:02"), 15.);
        assert_eq!(res, None);

        // new period
        let res = sampler.next_bar(date("2015-01-01 10:45:02"), 45.);
        assert_eq!(
            res,
            Some(Bars::WithEmpty(
                Bar {
                    open: 15.,
                    high: 16.,
                    low: 15.,
                    close: 15.,
                    stop_dt: date("2015-01-01 10:30:00")
                },
                vec![Bar {
                    open: 15.,
                    high: 15.,
                    low: 15.,
                    close: 15.,
                    stop_dt: date("2015-01-01 10:45:00")
                }]
            ))
        );
    }

    #[test]
    fn test_h12() {
        let mut sampler = H12::default();
        let res = sampler.next_bar(date("2015-01-01 01:03:00"), 0.);
        assert_eq!(res, None);
        let res = sampler.next_bar(date("2015-01-01 01:04:00"), 4.);
        assert_eq!(res, None);

        // new period start, should return prev period closing value
        let res = sampler.next_bar(date("2015-01-01 12:00:00"), 15.);
        assert_eq!(
            res,
            Some(Bars::Single(Bar {
                open: 0.,
                high: 4.,
                low: 0.,
                close: 4.,
                stop_dt: date("2015-01-01 12:00:00")
            }))
        );

        // 12-24 period hasn't passed, should return last period close value
        let res = sampler.next_bar(date("2015-01-01 13:00:00"), 15.);
        assert_eq!(res, None);

        // new period
        let res = sampler.next_bar(date("2015-01-03 10:45:02"), 45.);
        assert_eq!(
            res,
            Some(Bars::WithEmpty(
                Bar {
                    open: 15.,
                    high: 15.,
                    low: 15.,
                    close: 15.,
                    stop_dt: date("2015-01-02 00:00:00")
                },
                vec![
                    Bar {
                        open: 15.,
                        high: 15.,
                        low: 15.,
                        close: 15.,
                        stop_dt: date("2015-01-02 12:00:00")
                    },
                    Bar {
                        open: 15.,
                        high: 15.,
                        low: 15.,
                        close: 15.,
                        stop_dt: date("2015-01-03 00:00:00")
                    },
                ]
            ))
        );
    }

    #[test]
    fn test_d1() {
        let mut sampler = D1::default();
        let res = sampler.next_bar(date("2015-01-03 10:45:02"), 0.);
        assert_eq!(res, None);

        let res = sampler.next_bar(date("2015-01-04 00:00:00"), 1.);
        assert_eq!(
            res,
            Some(Bars::Single(Bar {
                open: 0.,
                high: 0.,
                low: 0.,
                close: 0.,
                stop_dt: date("2015-01-04 00:00:00")
            }))
        );

        let res = sampler.next_bar(date("2015-01-07 00:00:00"), 2.);
        // 05 and 06 are empty
        assert_eq!(
            res,
            Some(Bars::WithEmpty(
                Bar {
                    open: 1.,
                    high: 1.,
                    low: 1.,
                    close: 1.,
                    stop_dt: date("2015-01-05 00:00:00")
                },
                vec![
                    Bar {
                        open: 1.,
                        high: 1.,
                        low: 1.,
                        close: 1.,
                        stop_dt: date("2015-01-06 00:00:00")
                    },
                    Bar {
                        open: 1.,
                        high: 1.,
                        low: 1.,
                        close: 1.,
                        stop_dt: date("2015-01-07 00:00:00")
                    },
                ]
            ))
        )
    }

    #[test]
    fn test_w1() {
        let mut sampler = W1::default();
        // monday
        let res = sampler.next_bar(date("2021-01-04 10:45:02"), 0.);
        assert_eq!(res, None);

        // tuesday
        let res = sampler.next_bar(date("2021-01-05 00:00:00"), 1.);
        assert_eq!(res, None);

        // The next_bar monday
        let res = sampler.next_bar(date("2021-01-11 00:00:00"), 2.);
        assert_eq!(
            res,
            Some(Bars::Single(Bar {
                open: 0.,
                high: 1.,
                low: 0.,
                close: 1.,
                stop_dt: date("2021-01-11 00:00:00")
            }))
        );

        // Two weeks later, tuesday
        let res = sampler.next_bar(date("2021-01-26 00:00:00"), 3.);
        assert_eq!(
            res,
            Some(Bars::WithEmpty(
                Bar {
                    open: 2.,
                    high: 2.,
                    low: 2.,
                    close: 2.,
                    stop_dt: date("2021-01-18 00:00:00")
                },
                vec![Bar {
                    open: 2.,
                    high: 2.,
                    low: 2.,
                    close: 2.,
                    stop_dt: date("2021-01-25 00:00:00")
                }]
            ))
        );
    }

    #[test]
    fn test_mn1() {
        let mut sampler = MN1::default();
        let res = sampler.next_bar(date("2020-01-01 10:45:02"), 0.);
        assert_eq!(res, None);

        let res = sampler.next_bar(date("2020-01-02 00:00:00"), 1.);
        assert_eq!(res, None);

        let res = sampler.next_bar(date("2020-02-02 00:00:00"), 2.);
        assert_eq!(
            res,
            Some(Bars::Single(Bar {
                open: 0.,
                high: 1.,
                low: 0.,
                close: 1.,
                stop_dt: date("2020-02-01 00:00:00")
            }))
        );

        let res = sampler.next_bar(date("2020-10-26 00:00:00"), 3.);
        assert_eq!(
            res,
            Some(Bars::WithEmpty(
                Bar {
                    open: 2.,
                    high: 2.,
                    low: 2.,
                    close: 2.,
                    stop_dt: date("2020-03-01 00:00:00")
                },
                vec![
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        stop_dt: date("2020-04-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        stop_dt: date("2020-05-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        stop_dt: date("2020-06-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        stop_dt: date("2020-07-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        close: 2.,
                        low: 2.,
                        stop_dt: date("2020-08-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        stop_dt: date("2020-09-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        stop_dt: date("2020-10-01 00:00:00")
                    },
                ]
            ))
        );

        let res = sampler.next_bar(date("2021-01-01 00:00:01"), 3.);
        assert_eq!(
            res,
            Some(Bars::WithEmpty(
                Bar {
                    open: 3.,
                    high: 3.,
                    low: 3.,
                    close: 3.,
                    stop_dt: date("2020-11-01 00:00:00")
                },
                vec![
                    Bar {
                        open: 3.,
                        high: 3.,
                        low: 3.,
                        close: 3.,
                        stop_dt: date("2020-12-01 00:00:00")
                    },
                    Bar {
                        open: 3.,
                        high: 3.,
                        low: 3.,
                        close: 3.,
                        stop_dt: date("2021-01-01 00:00:00")
                    },
                ]
            ))
        );
    }

    fn date(date_str: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S").unwrap()
    }
}
