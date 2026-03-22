use std::time;

// Provides a convenience method to round time to specific resolution.
pub(crate) trait TimeRound {
    type Output;

    fn round(&self, dur: time::Duration) -> Option<Self::Output>;
}

impl TimeRound for time::SystemTime {
    type Output = time::SystemTime;

    fn round(&self, dur: time::Duration) -> Option<Self::Output> {
        let since_epoch = self.duration_since(time::SystemTime::UNIX_EPOCH).ok()?;
        let rounded = since_epoch.as_nanos() - since_epoch.as_nanos() % dur.as_nanos();
        let since_epoch = time::Duration::new(
            (rounded / time::Duration::from_secs(1).as_nanos()) as u64,
            (rounded % time::Duration::from_secs(1).as_nanos()) as u32,
        );

        time::SystemTime::UNIX_EPOCH.checked_add(since_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::TimeRound;
    use std::time;

    #[test]
    fn time_round() {
        let t = time::SystemTime::UNIX_EPOCH + time::Duration::new(1, 123456789);
        let expected = time::SystemTime::UNIX_EPOCH + time::Duration::new(1, 123000000);
        let rounded = t
            .round(time::Duration::from_millis(1))
            .expect("failed to round to millis");

        assert_eq!(expected, rounded);
    }
}
