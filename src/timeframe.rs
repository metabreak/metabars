use chrono::prelude::*;

#[derive(Debug, PartialEq)]
pub struct Bar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub bar_start: NaiveDateTime,
    pub next_bar_dt: NaiveDateTime,
}

impl From<State> for Bar {
    fn from(state: State) -> Self {
        Self {
            open: state.open,
            high: state.high,
            low: state.low,
            close: state.close,
            bar_start: state.bar_start,
            next_bar_dt: state.next_bar_dt,
        }
    }
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
    fn bar_start(&self, dt: NaiveDateTime) -> NaiveDateTime;

    fn next_bar(&mut self, dt: NaiveDateTime, value: f64) -> Option<Bars>;

    fn next_bar_dt(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime;

    fn current_incomplete(&self) -> Option<Bar>;
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

#[derive(Debug, Clone)]
struct State {
    bar_start: NaiveDateTime,
    next_bar_dt: NaiveDateTime,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

impl State {
    fn new(
        bar_start: NaiveDateTime,
        next_bar_dt: NaiveDateTime,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
    ) -> Self {
        Self {
            bar_start,
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
        fn current_incomplete(&self) -> Option<Bar> {
            self.state.to_owned().map(Bar::from)
        }

        fn next_bar(&mut self, dt: NaiveDateTime, value: f64) -> Option<Bars> {
            match self.state {
                Some(State {
                    bar_start,
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
                            bar_start,
                            next_bar_dt,
                        };

                        let mut empty_bar_start = next_bar_dt;
                        let mut empty_bar_end = self.next_bar_dt(next_bar_dt);

                        let mut empty_bars = vec![];
                        while dt >= empty_bar_end {
                            empty_bars.push(Bar {
                                open: close,
                                high: close,
                                low: close,
                                close,
                                bar_start: empty_bar_start,
                                next_bar_dt: empty_bar_end,
                            });
                            empty_bar_start = empty_bar_end;
                            empty_bar_end = self.next_bar_dt(empty_bar_end);
                        }

                        self.state = Some(State::new(
                            empty_bar_start,
                            empty_bar_end,
                            value,
                            value,
                            value,
                            value,
                        ));

                        if empty_bars.len() > 0 {
                            Some(Bars::WithEmpty(full_bar, empty_bars))
                        } else {
                            Some(Bars::Single(full_bar))
                        }
                    } else {
                        let high = f64::max(value, high);
                        let low = f64::min(value, low);
                        let close = value;

                        self.state =
                            Some(State::new(bar_start, next_bar_dt, open, high, low, close));
                        None
                    }
                }
                None => {
                    let next_bar_dt = self.next_bar_dt(dt);
                    self.state = Some(State::new(
                        self.bar_start(dt),
                        next_bar_dt,
                        value,
                        value,
                        value,
                        value,
                    ));
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

            fn bar_start(&self, dt: NaiveDateTime) -> NaiveDateTime {
                NaiveDate::from_ymd(dt.year(), dt.month(), dt.day()).and_hms(
                    dt.hour(),
                    (dt.minute() / $period) * $period,
                    0,
                )
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

            fn bar_start(&self, dt: NaiveDateTime) -> NaiveDateTime {
                NaiveDate::from_ymd(dt.year(), dt.month(), dt.day()).and_hms(
                    (dt.hour() / $period) * $period,
                    0,
                    0,
                )
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

    fn bar_start(&self, dt: NaiveDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd(dt.year(), dt.month(), dt.day()).and_hms(0, 0, 0)
    }
}

sampler!(W1);
impl Sampler for W1 {
    next!();

    fn next_bar_dt(&self, dt: NaiveDateTime) -> chrono::NaiveDateTime {
        let weekday = dt.weekday();
        let sub = weekday.num_days_from_monday() as i64;
        let add = 7 - sub;
        dt.date()
            .checked_add_signed(chrono::Duration::days(add))
            .unwrap()
            .and_hms(0, 0, 0)
    }

    fn bar_start(&self, dt: NaiveDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd(dt.year(), dt.month(), dt.day())
            .and_hms(0, 0, 0)
            .checked_sub_signed(chrono::Duration::days(
                dt.weekday().number_from_monday() as i64 - 1,
            ))
            .unwrap()
    }
}

sampler!(Mn1);
impl Sampler for Mn1 {
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

    // FIXME: fails on 0 year but who cares?
    fn bar_start(&self, dt: NaiveDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd(dt.year(), dt.month(), 1).and_hms(0, 0, 0)
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
        assert_eq!(
            sampler.current_incomplete(),
            Some(Bar {
                open: 0.,
                high: 0.,
                low: 0.,
                close: 0.,
                bar_start: date("2015-01-01 10:00:00"),
                next_bar_dt: date("2015-01-01 10:15:00")
            })
        );

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
                bar_start: date("2015-01-01 10:00:00"),
                next_bar_dt: date("2015-01-01 10:15:00")
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
                    bar_start: date("2015-01-01 10:15:00"),
                    next_bar_dt: date("2015-01-01 10:30:00")
                },
                vec![Bar {
                    open: 15.,
                    high: 15.,
                    low: 15.,
                    close: 15.,
                    bar_start: date("2015-01-01 10:30:00"),
                    next_bar_dt: date("2015-01-01 10:45:00")
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
                bar_start: date("2015-01-01 00:00:00"),
                next_bar_dt: date("2015-01-01 12:00:00")
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
                    bar_start: date("2015-01-01 12:00:00"),
                    next_bar_dt: date("2015-01-02 00:00:00")
                },
                vec![
                    Bar {
                        open: 15.,
                        high: 15.,
                        low: 15.,
                        close: 15.,
                        bar_start: date("2015-01-02 00:00:00"),
                        next_bar_dt: date("2015-01-02 12:00:00")
                    },
                    Bar {
                        open: 15.,
                        high: 15.,
                        low: 15.,
                        close: 15.,
                        bar_start: date("2015-01-02 12:00:00"),
                        next_bar_dt: date("2015-01-03 00:00:00")
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
                bar_start: date("2015-01-03 00:00:00"),
                next_bar_dt: date("2015-01-04 00:00:00")
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
                    bar_start: date("2015-01-04 00:00:00"),
                    next_bar_dt: date("2015-01-05 00:00:00")
                },
                vec![
                    Bar {
                        open: 1.,
                        high: 1.,
                        low: 1.,
                        close: 1.,
                        bar_start: date("2015-01-05 00:00:00"),
                        next_bar_dt: date("2015-01-06 00:00:00")
                    },
                    Bar {
                        open: 1.,
                        high: 1.,
                        low: 1.,
                        close: 1.,
                        bar_start: date("2015-01-06 00:00:00"),
                        next_bar_dt: date("2015-01-07 00:00:00")
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
                bar_start: date("2021-01-04 00:00:00"),
                next_bar_dt: date("2021-01-11 00:00:00")
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
                    bar_start: date("2021-01-11 00:00:00"),
                    next_bar_dt: date("2021-01-18 00:00:00")
                },
                vec![Bar {
                    open: 2.,
                    high: 2.,
                    low: 2.,
                    close: 2.,
                    bar_start: date("2021-01-18 00:00:00"),
                    next_bar_dt: date("2021-01-25 00:00:00")
                }]
            ))
        );
    }

    #[test]
    fn test_mn1() {
        let mut sampler = Mn1::default();
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
                bar_start: date("2020-01-01 00:00:00"),
                next_bar_dt: date("2020-02-01 00:00:00")
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
                    bar_start: date("2020-02-01 00:00:00"),
                    next_bar_dt: date("2020-03-01 00:00:00")
                },
                vec![
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        bar_start: date("2020-03-01 00:00:00"),
                        next_bar_dt: date("2020-04-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        bar_start: date("2020-04-01 00:00:00"),
                        next_bar_dt: date("2020-05-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        bar_start: date("2020-05-01 00:00:00"),
                        next_bar_dt: date("2020-06-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        bar_start: date("2020-06-01 00:00:00"),
                        next_bar_dt: date("2020-07-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        close: 2.,
                        low: 2.,
                        bar_start: date("2020-07-01 00:00:00"),
                        next_bar_dt: date("2020-08-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        bar_start: date("2020-08-01 00:00:00"),
                        next_bar_dt: date("2020-09-01 00:00:00")
                    },
                    Bar {
                        open: 2.,
                        high: 2.,
                        low: 2.,
                        close: 2.,
                        bar_start: date("2020-09-01 00:00:00"),
                        next_bar_dt: date("2020-10-01 00:00:00")
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
                    bar_start: date("2020-10-01 00:00:00"),
                    next_bar_dt: date("2020-11-01 00:00:00")
                },
                vec![
                    Bar {
                        open: 3.,
                        high: 3.,
                        low: 3.,
                        close: 3.,
                        bar_start: date("2020-11-01 00:00:00"),
                        next_bar_dt: date("2020-12-01 00:00:00")
                    },
                    Bar {
                        open: 3.,
                        high: 3.,
                        low: 3.,
                        close: 3.,
                        bar_start: date("2020-12-01 00:00:00"),
                        next_bar_dt: date("2021-01-01 00:00:00")
                    },
                ]
            ))
        );
    }

    fn date(date_str: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S").unwrap()
    }
}
