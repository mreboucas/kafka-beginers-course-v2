package com.br.mreboucas.openmind.kafka.tutorial3;

import org.joda.time.DateTime;

//https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/overview#:~:text=At%20Twitter%20we%20serve%20many,metadata%20shared%20by%20the%20user.
public class TweetDto {

    String created_at;

    String text;
    String date = DateTime.now().toString();
    public UserDto user;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public UserDto getUser() {
        return user;
    }

    public void setUser(UserDto user) {
        this.user = user;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public class UserDto {
        String name;

        String description;

        public String getFollowers_count() {
            return followers_count;
        }

        public void setFollowers_count(String followers_count) {
            this.followers_count = followers_count;
        }

        String followers_count;

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getName() {
            return name;
        }


    }
}
