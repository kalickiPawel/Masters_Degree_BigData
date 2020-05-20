package exercise06;

public class Movie {
    private String title;
    private long movieId;
    private String genres;

    public Movie() {
    }

    public Movie(String genres, long movieId, String title) {
        this.setTitle(title);
        this.setGenres(genres);
        this.setMovieId(movieId);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String newTitle) {
        this.title = newTitle;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String newGenres) {
        this.genres = newGenres;
    }

    public long getMovieId() {
        return movieId;
    }

    public void setMovieId(long newMovieId) {
        this.movieId = newMovieId;
    }
}
