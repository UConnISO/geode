def main():
    """Main function that does all the work; run searches, process results"""

    # For each of the searches, run the search and process the results
    # from that search
    for s in searches:
        results = splunk.search(s)
        process_results(results)


def process_results(results):
    """Process results from Splunk, inserting them into the database"""

    # For each of the results:
    for r in results:
        # First check to see if there is another event that spans this time
        # in the database
        lookup = db.get(r)

        # If there is, and the event matches, then merge these events and
        # update the database
        if r.matches(lookup):
            m = lookup.merge(r)
            db.update(m)
        # Otherwise, the information is conflicting, so terminate the old event
        # and make a new one. The termination happens at the start time of the
        # new event, since this is the first time that we know for certain
        # the old event does not match
        else:
            db.terminate(lookup, r.get('start'))
            db.insert(r)

        # Update the config file with the most recent time that we've processed
        _update_most_recent_time()


if __name__ == "__main__":
    main()
