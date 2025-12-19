pub trait EnsureResultExt<T, E> {
    fn ensure_or<P>(self, predicate: P, error: E) -> Result<T, E>
    where
        P: FnOnce(&T) -> bool;

    fn ensure_or_else<F, P>(self, predicate: P, error: F) -> Result<T, E>
    where
        F: FnOnce(&T) -> E,
        P: FnOnce(&T) -> bool;
}

impl<T, E> EnsureResultExt<T, E> for Result<T, E> {
    fn ensure_or<P>(self, predicate: P, error: E) -> Result<T, E>
    where
        P: FnOnce(&T) -> bool,
    {
        self.and_then(|value| {
            if predicate(&value) {
                Ok(value)
            } else {
                Err(error)
            }
        })
    }

    fn ensure_or_else<F, P>(self, predicate: P, error: F) -> Result<T, E>
    where
        F: FnOnce(&T) -> E,
        P: FnOnce(&T) -> bool,
    {
        self.and_then(|value| {
            if predicate(&value) {
                Ok(value)
            } else {
                Err(error(&value))
            }
        })
    }
}

pub trait EnsureOptionExt<T> {
    fn ensure<P>(self, predicate: P) -> Option<T>
    where
        P: FnOnce(&T) -> bool;
}

impl<T> EnsureOptionExt<T> for Option<T> {
    fn ensure<P>(self, predicate: P) -> Option<T>
    where
        P: FnOnce(&T) -> bool,
    {
        self.and_then(|value| if predicate(&value) { Some(value) } else { None })
    }
}

pub trait PureResultExt<T, E> {
    fn pure(t: T) -> Result<T, E>;
}

impl<T, E> PureResultExt<T, E> for Result<T, E> {
    fn pure(t: T) -> Result<T, E> {
        Ok(t)
    }
}

pub trait PureOptionExt<T> {
    fn pure(t: T) -> Option<T>;
}

impl<T> PureOptionExt<T> for Option<T> {
    fn pure(t: T) -> Option<T> {
        Some(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_or() {
        let result = Ok(42);
        let predicate = |value: &i32| *value == 42;
        let error = "Value is not 42";

        assert_eq!(result.ensure_or(predicate, error), Ok(42));

        let result = Ok(43);
        assert_eq!(result.ensure_or(predicate, error), Err("Value is not 42"));

        let result = Err("Error");
        assert_eq!(result.ensure_or(predicate, error), Err("Error"));
    }

    #[test]
    fn test_ensure_or_else() {
        let result = Ok(42);
        let predicate = |value: &i32| *value == 42;
        let error = |value: &i32| format!("Value is not 42, but {value}");

        assert_eq!(result.ensure_or_else(predicate, error), Ok(42));

        let result = Ok(43);
        assert_eq!(
            result.ensure_or_else(predicate, error),
            Err("Value is not 42, but 43".to_string())
        );

        let result = Err("Error".to_string());
        assert_eq!(
            result.ensure_or_else(predicate, error),
            Err("Error".to_string())
        );
    }

    #[test]
    fn test_ensure() {
        let option = Some(42);
        let predicate = |value: &i32| *value == 42;

        assert_eq!(option.ensure(predicate), Some(42));

        let option = Some(43);
        assert_eq!(option.ensure(predicate), None);

        let option = None;
        assert_eq!(option.ensure(predicate), None);
    }

    #[test]
    fn test_pure_result() {
        assert_eq!(Result::<i32, &str>::pure(42), Ok(42));
    }

    #[test]
    fn test_pure_option() {
        assert_eq!(Option::<i32>::pure(42), Some(42));
    }
}
